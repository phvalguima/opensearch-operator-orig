# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manage backup configurations.

This class must load the opensearch plugin: repository-s3 and configure it.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from charms.data_platform_libs.v0.s3 import S3Requirer
from charms.opensearch.v0.constants_charm import OPENSEARCH_REPOSITORY_NAME
from charms.opensearch.v0.opensearch_exceptions import (
    OpenSearchBackupRestoreError,
    OpenSearchKeystoreError,
    OpenSearchPluginError,
)
from charms.opensearch.v0.helper_enums import BaseStrEnum

from charms.opensearch.v0.opensearch_plugins import OpenSearchPlugin
from ops.framework import Object
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, StatusBase

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


class BackupServiceState(BaseStrEnum):
    """Enum for the states possible once plugin is enabled."""

    SUCCESS = "success"
    RESPONSE_FAILED_NETWORK = "response failed: network error"
    RESPONSE_FAILED_UNKNOWN = "response failed: unknown reason"
    REPO_NOT_CREATED = "repository not created"
    REPO_MISSING = "repository is missing from request"
    REPO_S3_UNREACHABLE = "repository s3 is unreachable"
    ILLEGAL_ARGUMENT = "request contained wrong argument"
    SNAPSHOT_MISSING = "snapshot not found"
    SNAPSHOT_RESTORE_ERROR = "restore of snapshot failed"
    SNAPSHOT_IN_PROGRESS = "snapshot in progress"
    SNAPSHOT_PARTIALLY_TAKEN = "snapshot partial: at least one shard missing"
    SNAPSHOT_INCOMPATIBILITY = "snapshot failed: incompatibility issues"
    SNAPSHOT_FAILED_UNKNOWN = "snapshot failed for unknown reason"


class OpenSearchBackupPlugin(OpenSearchPlugin):
    """Implements backup plugin."""

    def __init__(self, name: str):
        """Manager of OpenSearch client relations."""
        super().__init__(name)

    @property
    def depends_on(self) -> List[str]:
        """Returns the dependency list for this plugin."""
        return []

    def upgrade(self, uri: str) -> None:
        """Runs the upgrade process in this plugin."""
        raise NotImplementedError

    def is_enabled(self) -> bool:
        """Returns True if the plugin is enabled."""
        return self.is_installed() and self.is_related() and self.snapshot_status()

    def disable(self) -> bool:
        """Disables the plugin."""
        if not self.is_enabled():
            logger.warn("Depart event received but plugin not enabled yet.")
            raise OpenSearchBackupRestoreError("Depart event received but plugin not enabled yet.")
        s3_credentials = self.s3_client.get_s3_connection_info()
        return self.uninstall(
            "repository-s3",
            {
                **S3_OPENSEARCH_EXTRA_VALUES,
                "s3.client.default.region": s3_credentials["region"],
                "s3.client.default.endpoint": s3_credentials["endpoint"],
            },
            keystore={
                "s3.client.default.access_key": s3_credentials["access-key"],
                "s3.client.default.secret_key": s3_credentials["secret-key"],
            },
        )

    def enable(self) -> bool:
        """Enables the plugin."""
        restart_needed = False
        if self._check_missing_s3_config_completeness():
            BlockedStatus(f"Missing s3 info: {self._check_missing_s3_config_completeness()}")
            raise OpenSearchBackupRestoreError(
                f"Missing s3 info: {self._check_missing_s3_config_completeness()}"
            )
        if not self.is_installed():
            restart_needed = self.install("repository-s3") or restart_needed
        s3_credentials = self.s3_client.get_s3_connection_info()
        restart_needed = (
            self.configure(
                {
                    **S3_OPENSEARCH_EXTRA_VALUES,
                    "s3.client.default.region": s3_credentials["region"],
                    "s3.client.default.endpoint": s3_credentials["endpoint"],
                },
                keystore={
                    "s3.client.default.access_key": s3_credentials["access-key"],
                    "s3.client.default.secret_key": s3_credentials["secret-key"],
                },
            )
            or restart_needed
        )
        return restart_needed

    def check_missing_s3_config_completeness(self) -> List[str]:
        return [
            config
            for config in ["region", "bucket", "access-key", "secret-key"]
            if config not in self.s3_client.get_s3_connection_info()
        ]

    def get_service_status(self, response: Dict[str, Any]) -> BackupServiceState:
        """Returns the response status in a Enum.

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
        except KeyError as e:
            logger.exception(e)
            logger.error("response contained unknown error code")
            return BackupServiceState.RESPONSE_FAILED_UNKNOWN
        # Check if we error'ed b/c s3 repo is not configured, hence we are still
        # waiting for the plugin to be configured
        if type == "repository_exception" and REPO_NOT_CREATED_ERR in reason:
            return BackupServiceState.REPO_NOT_CREATED
        if type == "repository_missing_exception":
            return BackupServiceState.REPO_MISSING
        if type == "repository_verification_exception" and REPO_NOT_ACCESS_ERR in reason:
            return BackupServiceState.REPO_S3_UNREACHABLE
        if type == "illegal_argument_exception":
            return BackupServiceState.ILLEGAL_ARGUMENT
        if type == "snapshot_missing_exception":
            return BackupServiceState.SNAPSHOT_MISSING
        if type == "snapshot_restore_exception":
            return BackupServiceState.SNAPSHOT_RESTORE_ERROR

        # Now, check snapshot status:
        if "SUCCESS" in response:
            return BackupServiceState.SUCCESS
        if "IN_PROGRESS" in response:
            return BackupServiceState.SNAPSHOT_IN_PROGRESS
        if "PARTIAL" in response:
            return BackupServiceState.SNAPSHOT_PARTIALLY_TAKEN
        if "INCOMPATIBLE" in response:
            return BackupServiceState.SNAPSHOT_INCOMPATIBILITY
        if "FAILED" in response:
            return BackupServiceState.SNAPSHOT_FAILED_UNKNOWN
        return BackupServiceState.SUCCESS
