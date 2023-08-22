"""Manage backup configurations and actions.

This class must load the opensearch plugin: s3-repository and configure it.
"""

import logging
from typing import Dict, List, Optional, Union, Any

from charms.opensearch.v0.opensearch_plugins import OpenSearchPlugin
from charms.data_platform_libs.v0.s3 import S3Requirer
from charms.opensearch.v0.constants_charm import OPENSEARCH_REPOSITORY_NAME
from ops.framework import Object
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    StatusBase,
    MaintenanceStatus
)
from charms.opensearch.v0.opensearch_exceptions import (
    OpenSearchPluginError,
    OpenSearchKeystoreError,
    OpenSearchUnknownBackupRestoreError
)

# The unique Charmhub library identifier, never change it
LIBID = "d301deee4d2c4c1b8e30cd3df8034be2"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

logger = logging.getLogger(__name__)


S3_OPENSEARCH_EXTRA_VALUES = {
    "s3.client.default.max_retries": 3,
    "s3.client.default.path_style_access": False,
    "s3.client.default.protocol": "https",
    "s3.client.default.read_timeout": "50s",
    "s3.client.default.use_throttle_retries": True,
}
S3_REPO_BASE_PATH = "/"


REPO_NOT_CREATED_ERR = "repository type [s3] does not exist"
REPO_NOT_ACCESS_ERR = (
    f"[{OPENSEARCH_REPOSITORY_NAME}] path " + f"[{S3_REPO_BASE_PATH}]"
    if S3_REPO_BASE_PATH
    else "" + " is not accessible"
)


class OpenSearchBackup(OpenSearchPlugin):
    """Implements backup plugin."""

    def __init__(self, name: str, charm: Object, relname: Optional[str]):
        """Manager of OpenSearch client relations."""
        super().__init__(charm, name, charm, relname)

        # s3 relation handles the config options for s3 backups
        self.s3_client = S3Requirer(self.charm, relname)
        self.framework.observe(
            self.charm.on[relname].relation_departed, self._on_s3_credential_departed
        )
        self.framework.observe(
            self.s3_client.on.credentials_changed, self._on_s3_credential_changed
        )

    def upgrade(self, uri: str) -> None:
        """Runs the upgrade process in this plugin."""
        raise NotImplementedError

    def is_enabled(self) -> bool:
        """Returns True if the plugin is enabled."""
        return self.is_installed() and self.is_related() and self.snapshot_status()

    def disable(self) -> bool:
        """Disables the plugin."""
        return self._on_s3_credential_departed()

    def enable(self) -> bool:
        """Enables the plugin.

        There is no point in having enable with backup, as we are dependent on relation.
        """
        raise NotImplementedError

    def _check_s3_config_completeness(self) -> List[str]:
        return [
            config for config in ["region", "bucket", "access-key", "secret-key"]
            if config not in self.s3_client.get_s3_connection_info()
        ]

    def _on_s3_credential_departed(self, event) -> None:
        """Uninstalls the backup plugin."""
        restart_needed = False
        try:
            if not self.is_enabled():
                logger.warn("Depart event received but plugin not enabled yet.")
                return
            s3_credentials = self.s3_client.get_s3_connection_info()
            restart_needed = self.uninstall("repository-s3", {
                **S3_OPENSEARCH_EXTRA_VALUES,
                "s3.client.default.region": s3_credentials["region"],
                "s3.client.default.endpoint": s3_credentials["endpoint"]
            }, 
            keystore={
                "s3.client.default.access_key": s3_credentials["access-key"],
                "s3.client.default.secret_key": s3_credentials["secret-key"]
            })
        except (
            OpenSearchKeystoreError,
            OpenSearchPluginError,
        ) as e:
            logger.exception(e)
            logger.error("Error during backup setup")
            # Something went wrong
            event.defer()
            return
        if restart_needed:
            self.charm.on[self.charm.service_manager.name].acquire_lock.emit(
                callback_override="_restart_opensearch"
            )

    def _on_s3_credential_changed(self, event) -> None:
        """Sets credentials, resyncs if necessary and reports config errors.
        The first pass in this method should return false for the registration, then the
        backup plugin is installed and configured.
        """
        restart_needed = False
        try:
            if not self._check_s3_config_completeness():
                BlockedStatus(f"Missing s3 info: {self._check_s3_config_completeness()}")
                event.defer()
                return
            if not self.is_installed():
                restart_needed = restart_needed or self.install("repository-s3")
            s3_credentials = self.s3_client.get_s3_connection_info()
            restart_needed = restart_needed or self.configure({
                **S3_OPENSEARCH_EXTRA_VALUES,
                "s3.client.default.region": s3_credentials["region"],
                "s3.client.default.endpoint": s3_credentials["endpoint"]
                }, 
            keystore={
                "s3.client.default.access_key": s3_credentials["access-key"],
                "s3.client.default.secret_key": s3_credentials["secret-key"]
            })
        except (
            OpenSearchKeystoreError,
            OpenSearchPluginError,
        ) as e:
            logger.exception(e)
            logger.error("Error during backup setup")
            # Something went wrong
            event.defer()
            return
        if restart_needed:
            self.charm.on[self.charm.service_manager.name].acquire_lock.emit(
                callback_override="_restart_opensearch"
            )

    def _process_http_response(self, response: Dict[str, Any]) -> int:
        """Returns 0 if everything fine or a number higher than 0 otherwise.

        1: repository not found
        2: repository missing
        3: error while trying to access repository
        4: illegal argument exception
        5: missing snapshot
        6: error during restore
        7: unknown error
        
        Based on:
        https://github.com/opensearch-project/OpenSearch/blob/
            ba78d93acf1da6dae16952d8978de87cb4df2c61/
            server/src/main/java/org/opensearch/OpenSearchServerException.java#L837
        https://github.com/opensearch-project/OpenSearch/blob/
            ba78d93acf1da6dae16952d8978de87cb4df2c61/
            plugins/repository-s3/src/yamlRestTest/resources/rest-api-spec/test/repository_s3/40_repository_ec2_credentials.yml
        
        """
        try:
            if "error" not in response:
                return 0
            type = response["error"]["root_cause"][0]["type"]
            reason = response["error"]["root_cause"][0]["reason"]
            # Check if we error'ed b/c s3 repo is not configured, hence we are still
            # waiting for the plugin to be configured
            if type == "repository_exception" and REPO_NOT_CREATED_ERR in reason:
                logger.warn("repository_exception: repo [s3] not found")
                return 1
            if type == "repository_missing_exception":
                logger.warn("repository missing")
                return 2
            if type == "repository_verification_exception" and REPO_NOT_ACCESS_ERR in reason:
                logger.warn("repository_verification_exception: error trying to reach to s3")
                return 3
            if type == "illegal_argument_exception":
                logger.warn("illegal_argument_exception: wrong argument provided")
                return 4
            if type == "snapshot_missing_exception":
                logger.warn("snapshot_missing_exception: snapshot not found")
                return 5
            if type == "snapshot_restore_exception":
                logger.warn("snapshot_restore_exception: restore error")
                return 6
        except KeyError as e:
            logger.exception(e)
            logger.error("response contained unknown error code")
            return 7

    def _check_repo_status(self) -> Union[int, StatusBase]:
        try:
            response = self._request("GET", f"_snapshot/{OPENSEARCH_REPOSITORY_NAME}")
            logger.debug(f"_check_repo_status response: {response}")
            if self._process_http_response(response) > 0:
                return self._process_http_response(response), BlockedStatus("")
        except Exception as e:
            logger.exception(e)
            logger.error("Unknown backup failure")
            return BlockedStatus("backup service failed: unknown")

    def _check_snapshot_status(self) -> Union[int, StatusBase]:
        try:
            response = self._request("GET", "/_snapshot/_status")
            logger.debug(f"_check_snapshot_status response: {response}")
            if self._process_http_response(response) > 0:
                return self._process_http_response(response), BlockedStatus("")
        except Exception as e:
            logger.exception(e)
            logger.error("Unknown backup failure")
            return BlockedStatus("backup service failed: unknown")

        # Check state:
        if "SUCCESS" in response:
            return 10, ActiveStatus("")
        if "IN_PROGRESS" in response:
            return 11, MaintenanceStatus("backup in progress")
        if "PARTIAL" in response:
            return 12, BlockedStatus("partial backup: at least one shard failed to backup")
        if "INCOMPATIBLE" in response:
            return 13, BlockedStatus("backup failed: compatibility problems")
        if "FAILED" in response:
            return 14, BlockedStatus("backup service failed: unknown")
        return 0, ActiveStatus("")

    def check_if_snapshot_repo_created(self, bucket_name: str = "") -> bool:
        """Returns True if the snapshot repo has already been created.

        If bucket_name is set, then compare it in the response as well.
        """
        get = self._request("GET", f"_snapshot/{OPENSEARCH_REPOSITORY_NAME}")
        try:
            # Check if we error'ed b/c of missing snapshot repo
            if self._process_http_response(get) != 2:
                raise OpenSearchUnknownBackupRestoreError("Snapshot repo is wrongly set")
            if (
                bucket_name
                and bucket_name not in get["charmed-s3-repository"]["settings"]["bucket"]
            ):
                # bucket name changed, recreate the repo
                return False
        except KeyError:
            # One of the error keys are not present, this is a deeper issue
            raise OpenSearchUnknownBackupRestoreError("Snapshot repo is wrongly set")
        return True

    def register_snapshot_repo(self) -> bool:
        """Registers the snapshot repo in the cluster."""
        info = self.s3_client.get_s3_connection_info()
        bucket_name = info["bucket"]
        try:
            if self.check_if_snapshot_repo_created(bucket_name):
                # We've already created the repo, leaving...
                return True
            put = self._request(
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
            return self._process_http_response(put)
        finally:
            # Unknown condition reached
            raise OpenSearchUnknownBackupRestoreError(
                "register_snapshot_repo - unknown"
            )

    def get_status(self) -> Union[int, StatusBase]:
        """Get the backup status.

        The possible status are for the super() status:
        0: blocked, not installed
        1: blocked, not enabled
        2: active
        3: blocked, waiting for an upgrade action
        """
        code, status = super().get_status()
        if code != 2:
            return code, status

        # Active status according to the base plugin, check backup specifics
        repo_code, repo_status = self._check_repo_status()
        if repo_code > 0:
            return 40 + repo_code, repo_status

        # Check snapshot status
        snapshot_code, snapshot_status = self._check_snapshot_status()
        if snapshot_code > 0:
            return 50 + snapshot_code, snapshot_status