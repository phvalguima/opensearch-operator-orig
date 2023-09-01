# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manages the backup events within the charm.

This file holds the implementation of the BackupCharm class, which listens to S3 and action
events related to backup/restore cycles.

The setup of s3-repository happens in two phases: (1) at credentials-changed event, where
the backup configuration is made in opensearch.yml and the opensearch-keystore; (2) when
the first action is requested and the actual registration of the repo takes place.

That needs to be separated in two phases as the configuration itself will demand a restart,
before configuring the actual snapshot repo is possible in OpenSearch.

The removal of backup only reverses step (1), to avoid accidentally deleting the existing
snapshots in the S3 repo.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from charms.data_platform_libs.v0.s3 import S3Requirer
from charms.opensearch.v0.constants_charm import OPENSEARCH_REPOSITORY_NAME
from charms.opensearch.v0.opensearch_exceptions import (
    OpenSearchKeystoreError,
    OpenSearchPluginError,
    OpenSearchHttpError,
    OpenSearchError
)
from charms.opensearch.v0.opensearch_backups import BackupServiceState, S3_REPO_BASE_PATH
from ops.framework import Object
from ops.model import ActiveStatus, WaitingStatus, ErrorStatus, BlockedStatus, MaintenanceStatus, StatusBase

# The unique Charmhub library identifier, never change it
LIBID = "d301deee4d2c4c1b8e30cd3df8034be2"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

logger = logging.getLogger(__name__)


class OpenSearchBackupRestoreError(OpenSearchError):
    """Exception thrown when an opensearch backup-related action fails."""

    def __init__(self, state: BackupServiceState, msg: str):
        super().__init__()
        self.msg = f"{msg}: {str(state)}"
        self.state = state

    def __str__(self):
        return self.msg


class OpenSearchBackup(Object):
    """Implements backup plugin."""

    def __init__(self, charm: Object, relname: str):
        """Manager of OpenSearch client relations."""
        super().__init__(charm, relname)
        self.charm = charm
        self.rel_name = relname
        # s3 relation handles the config options for s3 backups
        self.s3_client = S3Requirer(self.charm, relname)
        self.framework.observe(
            self.charm.on[relname].relation_departed, self.on_s3_change
        )
        self.framework.observe(
            self.s3_client.on.credentials_changed, self.on_s3_change
        )

    @property
    def is_enabled(self):
        """Checks the plugin is enabled."""
        if self.charm.model.get_relation(self.rel_name) is None:
            # missing relation
            return False
        # check if the plugin is configured correctly
        # Not trapping Errors as we've checked for missing relations before
        # If anything goes wrong, then we have an actual problem
        if not self.backup_plugin.is_enabled:
            return False
        return True

    @property
    def backup_plugin(self):
        if "repository-s3" in self.charm.plugins:
            return self.charm.plugins["repository-s3"]
        if self.charm.model.get_relation(self.rel_name) is None:
            raise OpenSearchBackupRestoreError("Backup plugin - missing S3 relation")
        raise OpenSearchBackupRestoreError("Backup plugin - unknown error when checking plugin manager")

    def on_s3_change(self, event) -> None:
        """Call the plugin manager config handler."""
        if self.charm.plugin_manager.on_config_change(event):
            self.charm.on[self.charm.service_manager.name].acquire_lock.emit(
                callback_override="_restart_opensearch"
            )

    def _request(self, *args, **kwargs) -> Dict[str, any]:
        """Returns the output of OpenSearchDistribution.request() or throws an error.

        Request method can return one of many: Union[Dict[str, any], List[any], int]
        If int is returned, then throws an exception informing the HTTP request failed.
        """
        try:
            result = self.charm.opensearch.request(args, kwargs)
        except OpenSearchHttpError as e:
            logger.exception(e)
            raise OpenSearchBackupRestoreError(
                BackupServiceState.RESPONSE_FAILED_NETWORK, str(e))
        if isinstance(result, int):
            raise OpenSearchBackupRestoreError(
                BackupServiceState.RESPONSE_FAILED_NETWORK,
                f"Request failed with code {result}")
        response_status = self.backup_plugin.get_response_status(result)
        if response_status != BackupServiceState.SUCCESS:
            # Another error that is not 
            raise OpenSearchBackupRestoreError(response_status, "")
        return result

    def _check_repo_status(self) -> Dict[str, any]:
        return self._request("GET", f"_snapshot/{OPENSEARCH_REPOSITORY_NAME}")

    def _check_snapshot_status(self) -> Union[int, StatusBase]:
        return self._request("GET", "/_snapshot/_status")

    def _check_if_snapshot_repo_created(self, bucket_name: str = "") -> bool:
        """Returns True if the snapshot repo has already been created.

        If bucket_name is set, then compare it in the response as well.
        """
        try:
            get = self._check_repo_status()
        except OpenSearchBackupRestoreError as e:
            if e.state != BackupServiceState.REPO_MISSING:
                raise e
            return False
        try:
            if (
                bucket_name
                and bucket_name not in get["charmed-s3-repository"]["settings"]["bucket"]
            ):
                # bucket name changed, recreate the repo
                return False
        except KeyError as e:
            # One of the error keys are not present, this is a deeper issue
            raise OpenSearchBackupRestoreError(BackupServiceState.RESPONSE_FAILED_UNKNOWN, str(e))
        return True

    def register_snapshot_repo(self) -> bool:
        """Registers the snapshot repo in the cluster."""
        info = self.s3_client.get_s3_connection_info()
        bucket_name = info["bucket"]
        if self._check_if_snapshot_repo_created(bucket_name):
            # We've already created the repo, leaving...
            return True
        self._request(
            "PUT",
            f"_snapshot/{OPENSEARCH_REPOSITORY_NAME}",
            payload={
                "type": "s3",
                "settings": {
                    "bucket": bucket_name,
                    "base_path": S3_REPO_BASE_PATH,
                },
            },
        )
        return True

    def status(self) -> StatusBase:
        """Returns a meaningful status to be used by the charm."""
        try:
            if not self.is_enabled:
                return WaitingStatus(f"Backup: waiting on plugin {self.backup_plugin.status()}")
            # Plugin configured, now check the status of the service
            self._check_repo_status()
            self._check_snapshot_status()
        except OpenSearchBackupRestoreError as e:
            msg = f"Backup: {str(e)}"
            if (e.state == BackupServiceState.ILLEGAL_ARGUMENT):
                return BlockedStatus(msg)
            if (e.state == BackupServiceState.REPO_NOT_CREATED or
                e.state == BackupServiceState.SNAPSHOT_IN_PROGRESS):
                return MaintenanceStatus(msg)
            return ErrorStatus(msg)
        return ActiveStatus("Backup is enabled")
