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

We must avoid having both lock types conceeding locks at the same time.
We also must avoid providing locks when the cluster has nodes shutdown.

For that, the charm leader needs to also check if remote units are up and
running and manage the lock accordingly. This monitoring should start once
the unit has finished its start-up process.
"""

import logging
from enum import Enum

from charms.opensearch.v0.constants_charm import (
    LockIsBlockedOnUnit,
    LockRetryLater,
    PeerRelationName,
)
from charms.opensearch.v0.opensearch_exceptions import (
    OpenSearchError,
    OpenSearchHttpError,
    OpenSearchOpsLockAlreadyAcquiredError,
)
from charms.opensearch.v0.opensearch_internal_data import Scope
from charms.rolling_ops.v0.rollingops import Lock, Locks, LockState, RollingOpsManager
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus
from tenacity import retry, stop_after_attempt, wait_fixed

# The unique Charmhub library identifier, never change it
LIBID = "0924c6d81c604a15873ad43498cd6895"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

logger = logging.getLogger(__name__)


class OpenSearchLockState(Enum):
    """Reports the status of a given OpenSearch node for locking purposes.

    Besides the original: ACQUIRE, RELEASE, GRANTED, IDLE, also count the cases:
    - DEPARTING: node has requested a lock via ops_lock during storage-detaching
    - ACQUIRE_WITH_SVC_STOPPED: requested the lock while its service is stopped
    - STOPPED_BUT_NOT_REQUESTED: the service is stopped, but the lock was not requested
    - RETRY_LOCK: the lock should be retried later
    """

    ACQUIRE = "acquire"
    RELEASE = "release"
    GRANTED = "granted"
    IDLE = "idle"

    # New states
    DEPARTING = "departing"
    RETRY_LOCK = "retry-lock"
    # TODO: the following states are not really necessary if shards are healthy.
    #       That means, it is not about the service being stopped, but rather
    #       if the current cluster can handle yet another node going away, and which.
    #       We should improve this method to check for the shard health.
    # Last Note: the cluster health is also not a good indicator. We may have
    #            to restart nodes EVEN if the health is not green.
    # For the moment: take the easy way out - not providing keys if we have stopped
    # nodes in the cluster AND these nodes are not requesting the lock.
    ACQUIRE_WITH_SVC_STOPPED = "acquire-with-svc-stopped"
    STOPPED_BUT_NOT_REQUESTED = "stopped-but-not-requested"


class OpenSearchRetryLockLaterException(OpenSearchError):
    """Exception thrown when the lock should be retried later."""


class OpenSearchLock(Lock):
    """Class for controlling the locks in OpenSearch Charm."""

    def __init__(self, manager, unit):
        super().__init__(manager, unit=unit)
        self._opensearch = manager.charm.opensearch
        self._ops_lock = manager.charm.ops_lock.is_held()
        self.charm = manager.charm
        # Run it once, so we can store the status of the unit.
        self.unit_is_up = self._opensearch.is_remote_node_up(self.unit)
        if self.charm.unit == self.unit and self.unit_is_up:
            self.started()

    @property
    def _state(self) -> OpenSearchLockState:
        """Method for getting the lock state."""
        if self._ops_lock:
            # Simple case, we will not process locks until the unit is removed.
            return OpenSearchLockState.DEPARTING

        app_state = OpenSearchLockState(super()._state.value)
        if app_state in [OpenSearchLockState.RELEASE, OpenSearchLockState.GRANTED]:
            return app_state

        # Now, we need to figure out if the extra status of the lock.
        # Currently, we know the lock is either ACQUIRE or IDLE.
        if self.retrial_count > 0:
            # The unit is requesting another retry.
            return OpenSearchLockState.RETRY_LOCK

        # Is the unit down?
        if not self.unit_is_up:
            if app_state == OpenSearchLockState.ACQUIRE:
                # The unit is requesting the lock.
                return OpenSearchLockState.ACQUIRE_WITH_SVC_STOPPED
            return OpenSearchLockState.STOPPED_BUT_NOT_REQUESTED

        # Return can either be originals ACQUIRE or IDLE
        if app_state == OpenSearchLockState.ACQUIRE:
            # The unit is requesting the lock.
            return OpenSearchLockState.ACQUIRE
        return OpenSearchLockState.IDLE

    @_state.setter
    def _state(self, s: LockState):
        """Method for setting the lock state.

        Although the lock may have more states, these are calculated at _state call.
        The states to be stored remains the same as the parent class.
        """
        state = OpenSearchLockState(s.value)
        if state in [
            OpenSearchLockState.ACQUIRE,
            OpenSearchLockState.DEPARTING,
            OpenSearchLockState.RETRY_LOCK,
            OpenSearchLockState.ACQUIRE_WITH_SVC_STOPPED,
        ]:
            self.relation.data[self.unit].update({"state": LockState.ACQUIRE.value})
        elif state == OpenSearchLockState.RELEASE:
            self.relation.data[self.unit].update({"state": LockState.RELEASE.value})
        elif state == OpenSearchLockState.GRANTED:
            self.relation.data[self.app].update({str(self.unit): LockState.GRANTED.value})
        elif state in [
            OpenSearchLockState.IDLE,
            OpenSearchLockState.STOPPED_BUT_NOT_REQUESTED,
        ]:
            self.relation.data[self.app].update({str(self.unit): LockState.IDLE.value})

    def is_blocked(self) -> bool:
        """Method for checking if the lock is blocked."""
        return self.has_started() and (
            self._state == OpenSearchLockState.DEPARTING
            or self._state == OpenSearchLockState.STOPPED_BUT_NOT_REQUESTED
        )

    def is_held(self):
        """This unit holds the lock."""
        return self._state == OpenSearchLockState.GRANTED

    def release_requested(self):
        """A unit has reported that they are finished with the lock."""
        return self._state == OpenSearchLockState.RELEASE

    def is_pending(self):
        """Is this unit waiting for a lock?"""
        return self._state in [
            OpenSearchLockState.ACQUIRE,
            OpenSearchLockState.DEPARTING,
            OpenSearchLockState.RETRY_LOCK,
            OpenSearchLockState.ACQUIRE_WITH_SVC_STOPPED,
        ]

    def has_started(self) -> bool:
        """Method for checking if the unit has started."""
        return self.relation.data[self.unit].get("has_started") == "True"

    def started(self):
        """Sets the started flag.

        Should be called once the unit has finished its start process.
        """
        self.relation.data[self.unit]["has_started"] = "True"

    def retry(self) -> bool:
        """Method for checking if the lock should be retried."""
        return self._state == OpenSearchLockState.RETRY_LOCK

    @property
    def retrial_count(self) -> int:
        """Method for getting the retrial count."""
        return int(self.relation.data[self.unit].get(OpenSearchLockState.RETRY_LOCK.value, 0))

    @retrial_count.setter
    def retrial_count(self, count: int):
        """Method for getting the retrial count."""
        self.relation.data[self.unit][OpenSearchLockState.RETRY_LOCK.value] = str(count)

    def acquire_with_stopped_service(self) -> bool:
        """Method for checking if the lock is acquired with the service stopped."""
        return self._state == OpenSearchLockState.ACQUIRE_WITH_SVC_STOPPED


class OpenSearchLocks(Locks):
    """Generator that returns a list of locks."""

    def __init__(self, manager):
        super().__init__(manager)

    def __iter__(self):
        """Yields a lock for each unit we can find on the relation."""
        for unit in self.units:
            yield OpenSearchLock(self.manager, unit=unit)


class OpenSearchRollingOpsManager(RollingOpsManager):
    """Class for controlling the locks in OpenSearch Charm.

    It differs from the main RollingOpsManager in two ways:
    1) It will take into account the OpenSearchOpsLock status before granting locks
    2) It will retry the lock acquisition if the restart-repeatable flag is set:
       that is used to indicate the unit requested the lock, but could not execute
       the operation because of a factor outside of its control. Use this resource
       whenever a given unit depends on the charm leader, for example, to progress.
    """

    def __init__(self, charm, relation, callback):
        """Constructor for the manager."""
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
                    {"callback_override": callback}
                )
            )

    def _on_acquire_lock(self, event):
        """Method for acquiring the lock. Restart the retry-lock counter."""
        OpenSearchLock(self, self.charm.unit).retrial_count = 0
        return super()._on_acquire_lock(event)

    def _should_lock_be_reacquired(self):
        """Method for checking if the restart should be retried now.

        For that, the unit has registered the restart-repeatable flag in the service
        relation data and the lock is not held or pending anymore.
        """
        lock = OpenSearchLock(self, self.charm.unit)
        return (
            # TODO: consider cases with a limitation in the amount of retries
            lock.retry()
            and not (lock.is_held() or lock.is_pending())
        )

    def _on_run_with_lock(self, event):
        """Method for running with lock."""
        lock = OpenSearchLock(self, self.charm.unit)
        try:
            super()._on_run_with_lock(event)
            self.charm.status.clear(LockRetryLater.format(self.name))
            return
        except OpenSearchRetryLockLaterException:
            logger.info("Retrying to acquire the lock later.")
            lock.retrial_count = lock.retrial_count + 1
        except Exception:
            raise

        # A retriable error happened, raised by the callback method
        # It means the logic after callback execution was not ran.
        # Release the lock now, so we can reissue it later
        lock.release()  # Updates relation data

        # cleanup old callback overrides:
        # we do not clean up the callback override, so we can reissue it later
        # self.relation.data[self.charm.unit].update({"callback_override": ""})
        if self.model.unit.status.message == f"Executing {self.name} operation":
            self.model.unit.status = ActiveStatus()
        self.charm.status.set(WaitingStatus(LockRetryLater.format(self.name)))

    def _on_process_locks(self, _):  # noqa: C901
        """Method for processing the locks.

        There are certain special rules to be considered when providing the lock:
        1) The node is trying to acquire it
        2) There is no node departing in the cluster
        3) There is no node with the service stopped in the cluster

        We build the lock following the original _on_process_locks scheme. Then,
        we should check the ops_lock and ensure it is not held. If it is, we abandon
        the event and wait for the next peer relation departed to reprocess.

        We check that each node is reachable and healthy.
        If not and node is requesting lock, then the it is set to: ACQUIRE_WITH_SVC_STOPPED

        Finally, if we have any of the following:
        1) Nodes helding this lock
        2) At least one node departing via ops_lock
        3) Nodes with stopped service that did not try to acquire the lock
        We abandon the process and do not restart the cluster any further.
        """
        if not self.charm.unit.is_leader():
            return

        pending = []

        # First pass:
        # Find if we can process locks or should we wait for the next event.
        # Build a list of units that are pending.
        for lock in OpenSearchLocks(self):
            if lock.is_held():
                # One of our units has the lock -- return without further processing.
                return

            if lock.release_requested():
                lock.clear()  # Updates relation data

            if lock.is_blocked():
                self.model.app.status = BlockedStatus(
                    LockIsBlockedOnUnit.format(self.name, lock.unit.name)
                )
                return

            if lock.is_pending():
                if lock.unit == self.model.unit:
                    # Always run on the leader last.
                    pending.insert(0, lock)
                else:
                    pending.append(lock)

        self.charm.status.clear(
            LockIsBlockedOnUnit[:-4].format(self.name),
            pattern=self.charm.status.CheckPattern.Start,
        )

        # Find the next lock we want to process. We check for lock priority
        # 1) Do we have any locks with: ACQUIRE_WITH_SVC_STOPPED
        # 2) Do we have any locks with: RETRY_LOCK
        # 3) Do we have any locks with: ACQUIRE (all the remaining)
        next_lock_to_process = None
        for lock in pending:
            # 1) Do we have any locks with: ACQUIRE_WITH_SVC_STOPPED
            if lock.acquire_with_stopped_service():
                next_lock_to_process = lock
                break

            # 2) Do we have any locks with: RETRY_LOCK
            if lock.retry():
                next_lock_to_process = lock
                break

        if not next_lock_to_process and pending:
            # 3) Do we have any locks with: ACQUIRE (all the remaining)
            next_lock_to_process = pending[-1]

        # If we reach this point, and we have pending units, we want to grant a lock to
        # one of them.
        if next_lock_to_process:
            self.model.app.status = MaintenanceStatus("Beginning rolling {}".format(self.name))
            next_lock_to_process.grant()
            if next_lock_to_process.unit == self.model.unit:
                # It's time for the leader to run with lock.
                self.charm.on[self.name].run_with_lock.emit()
            return

        if self.model.app.status.message == f"Beginning rolling {self.name}":
            self.model.app.status = ActiveStatus()


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
