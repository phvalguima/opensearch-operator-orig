# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Class for controlling the lock systems in OpenSearch Charm.

There are two types of locks within OpenSearch charm:
1) The standard rolling-ops library
2) The OpenSearchOpsLock

The former is used to control rolling operations across the cluster, where
we can reliably use the peer relation to orchestrate these activities. The
leader unit will keep the control and will grant the lock to the next unit
in the relation.

The latter is used to control the removal of units from the cluster. In this
case, the sensitive operations happen in the storage-detaching event, which
cannot be deferred or abandoned. This event will trigger a series of steps
that will flush data to disk and exclude the unit from any voting/allocation.

As everything happening in the storage-detaching must be atomic, we cannot
rely on the peer relation and events being triggered later on in the process
in other units. We must use the opensearch itself to store the lock info.
That assures any unit can access locking information at any time, even during
a storage-detaching event on a peer unit.

The last important point is that we must avoid having both lock types conceeding
locks at the same time. For that, the RollingOpsManager is overloaded here
and the new class will also take the status of OpenSearchOpsLock into account
before granting locks.
"""

import logging

from charms.opensearch.v0.constants_charm import LockRetryLater, PeerRelationName
from charms.opensearch.v0.opensearch_exceptions import (
    OpenSearchError,
    OpenSearchHttpError,
    OpenSearchOpsLockAlreadyAcquiredError,
)
from charms.opensearch.v0.opensearch_internal_data import Scope
from charms.rolling_ops.v0.rollingops import Lock, RollingOpsManager
from ops.model import WaitingStatus
from tenacity import retry, stop_after_attempt, wait_fixed

# The unique Charmhub library identifier, never change it
LIBID = "0924c6d81c604a15873ad43498cd6895"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

logger = logging.getLogger(__name__)


class OpenSearchRetryLockLaterException(OpenSearchError):
    """Exception thrown when the lock should be retried later."""


class RollingOpsManagerWithExclusions(RollingOpsManager):
    """Class for controlling the locks in OpenSearch Charm.

    It differs from the main RollingOpsManager in two ways:
    1) It will take into account the OpenSearchOpsLock status before granting locks
    2) It will retry the lock acquisition if the restart-repeatable flag is set:
       that is used to indicate the unit requested the lock, but could not execute
       the operation because of a factor outside of its control. Use this resource
       whenever a given unit depends on the charm leader, for example, to progress.
    """

    RETRY_LOCK = "retry-lock-counter"

    def __init__(self, charm, relation, callback):
        """Constructor for RollingOpsManagerWithExclusions."""
        super().__init__(charm, relation, callback)
        self.ops_lock = charm.ops_lock

        # Given the process_locks may abandon relation-changed events because
        # the ops_lock is being held, we must listen to more events.
        for event in [
            charm.on.update_status,
            charm.on[self.name].relation_departed,
        ]:
            self.framework.observe(event, self._on_relation_changed)

        self.relation = charm.model.get_relation(self.name)

        # Calling this here guarantees we will check, for each node, if we
        # should reissue a lock request on every hook.
        if self.relation and self._should_lock_be_reacquired():
            callback = self.relation.data[self.charm.unit].get("callback_override", "")
            charm.on[self.name].acquire_lock.emit(
                callback_override=self.relation.data[self.charm.unit].update(
                    {
                        "callback_override": callback
                    }
                )
            )

    def _on_acquire_lock(self, event):
        """Method for acquiring the lock. Restart the retry-lock counter."""
        self.relation.data[self.charm.model.unit][self.RETRY_LOCK] = "0"  # reset counter
        return super()._on_acquire_lock(event)

    def _should_lock_be_reacquired(self):
        """Method for checking if the restart should be retried now.

        For that, the unit has registered the restart-repeatable flag in the service
        relation data and the lock is not held or pending anymore.
        """
        return (
            # TODO: consider cases with a limitation in the amount of retries
            int(self.relation.data[self.charm.model.unit].get(self.RETRY_LOCK, 0)) > 0
            and not (Lock(self).is_held() or Lock(self).is_pending())
        )

    def _on_run_with_lock(self, event):
        """Method for running with lock."""
        try:
            super()._on_run_with_lock(event)
            if self.model.unit.status.message == LockRetryLater.format(self.name):
                self.charm.status.clear(LockRetryLater.format(self.name))
            return
        except OpenSearchRetryLockLaterException:
            logger.info("Retrying to acquire the lock later.")
            self.relation.data[self.charm.model.unit][self.RETRY_LOCK] = str(
                int(self.relation.data[self.charm.model.unit].get(self.RETRY_LOCK, 0)) + 1
            )
        except:
            raise

        # A retriable error happened, raised by the callback method
        # Release the lock now, so we can reissue it later
        lock = Lock(self)
        lock.release()  # Updates relation data
        # cleanup old callback overrides:
        # we do not clean up the callback override, so we can reissue it later
        # self.relation.data[self.charm.unit].update({"callback_override": ""})
        if self.model.unit.status.message == f"Executing {self.name} operation":
            self.charm.status.set(WaitingStatus(LockRetryLater.format(self.name)))

    def _on_process_locks(self, event):
        """Method for processing the locks.

        We should only grant a lock here if the ops_lock is free and then,
        check with the parent RollingOpsManager.

        We need to consider the fact that storage-detaching may be happening.
        In this case, we should not grant the lock until ops_lock is released.
        """
        if not self.charm.model.unit.is_leader():
            return

        if self.ops_lock.is_held():
            logger.info("Another unit is being removed, skipping the rolling ops.")
            return

        # Call the parent method.
        super()._on_process_locks(event)


class OpenSearchOpsLock:
    """This class covers the configuration changes depending on certain actions."""

    LOCK_INDEX = ".ops_lock"
    PEER_DATA_LOCK_FLAG = "ops_removing_unit"

    def __init__(self, charm):
        self._charm = charm
        self._opensearch = charm.opensearch

    def is_held(self):
        """Method for checking if the lock is held."""
        try:
            status_code = self._opensearch.request(
                "GET",
                endpoint=f"/{OpenSearchOpsLock.LOCK_INDEX}",
                host=self._charm.unit_ip if self._opensearch.is_node_up() else None,
                alt_hosts=self._charm.alt_hosts,
                retries=3,
                resp_status_code=True,
            )
            if status_code < 300:
                return True
        except OpenSearchHttpError as e:
            logger.warning(f"Error checking for ops_lock: {e}")
            pass
        return False

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(0.5), reraise=True)
    def acquire(self):
        """Method for Acquiring the "ops" lock."""
        # no lock acquisition needed if only 1 unit remaining
        if len(self._charm.model.get_relation(PeerRelationName).units) == 1:
            return

        # we check first on the peer data bag if the lock is already acquired
        if self._is_lock_in_peer_data():
            raise OpenSearchOpsLockAlreadyAcquiredError("Another unit is being removed.")

        host = self._charm.unit_ip if self._opensearch.is_node_up() else None

        # we can use opensearch to lock
        if host is not None or self._charm.alt_hosts:
            try:
                # attempt lock acquisition through index creation, should crash if index
                # already created, meaning another unit is holding the lock
                self._opensearch.request(
                    "PUT",
                    endpoint=f"/{OpenSearchOpsLock.LOCK_INDEX}",
                    host=host,
                    alt_hosts=self._charm.alt_hosts,
                    retries=3,
                )
                self._charm.peers_data.put(Scope.UNIT, OpenSearchOpsLock.PEER_DATA_LOCK_FLAG, True)
                return
            except OpenSearchHttpError as e:
                if e.response_code != 400:
                    raise
                raise OpenSearchOpsLockAlreadyAcquiredError("Another unit is being removed.")

        # we could not use opensearch for locking, we use the peer rel data bag
        self._charm.peers_data.put(Scope.UNIT, OpenSearchOpsLock.PEER_DATA_LOCK_FLAG, True)

    def release(self):
        """Method for Releasing the "ops" lock."""
        host = self._charm.unit_ip if self._opensearch.is_node_up() else None

        # can use opensearch to remove lock
        if host is not None or self._charm.alt_hosts:
            try:
                self._opensearch.request(
                    "DELETE",
                    endpoint=f"/{OpenSearchOpsLock.LOCK_INDEX}",
                    host=host,
                    alt_hosts=self._charm.alt_hosts,
                    retries=3,
                )
            except OpenSearchHttpError as e:
                # ignore 404, it means the index is not found and this just means that
                # the cleanup happened before but event got deferred because of another error
                if e.response_code != 404:
                    raise

        self._charm.peers_data.delete(Scope.UNIT, OpenSearchOpsLock.PEER_DATA_LOCK_FLAG)

    def _is_lock_in_peer_data(self) -> bool:
        """Method checking if lock acquired from the peer rel data."""
        rel = self._charm.model.get_relation(PeerRelationName)
        for unit in rel.units:
            if rel.data[unit].get(OpenSearchOpsLock.PEER_DATA_LOCK_FLAG) == "True":
                return True

        return False
