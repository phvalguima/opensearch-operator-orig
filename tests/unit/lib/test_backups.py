# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit test for the opensearch_plugins library."""
import unittest
from unittest.mock import patch, MagicMock

from charms.opensearch.v0.opensearch_backups import (
    OPENSEARCH_REPOSITORY_NAME,
    S3_OPENSEARCH_EXTRA_VALUES,
    S3_RELATION,
    S3_REPO_BASE_PATH,
)
from charms.opensearch.v0.opensearch_plugins import OpenSearchPluginConfig, PluginState
from ops.testing import Harness

from charm import OpenSearchOperatorCharm

TEST_BUCKET_NAME = "s3://bucket-test"


class TestBackups(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = Harness(OpenSearchOperatorCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()
        self.charm = self.harness.charm
        # Override the config to simulate the TestPlugin
        # As config.yaml does not exist, the setup below simulates it
        self.charm.plugin_manager._charm_config = self.harness.model._config
        self.plugin_manager = self.charm.plugin_manager
        # Replace some unused methods that will be called as part of set_leader with mock
        self.charm.service_manager._update_locks = MagicMock()
        self.charm._on_leader_elected = MagicMock()
        self.harness.set_leader(is_leader=True)

        # Relate and run first check
        with patch(
            "charms.opensearch.v0.opensearch_plugin_manager.OpenSearchPluginManager.run"
        ) as mock_pm_run:
            self.s3_rel_id = self.harness.add_relation(S3_RELATION, "s3-integrator")
            self.harness.add_relation_unit(self.s3_rel_id, "s3-integrator/0")
            mock_pm_run.assert_not_called()

    @patch("charms.opensearch.v0.opensearch_plugin_manager.OpenSearchPluginManager.status")
    @patch("charms.opensearch.v0.opensearch_backups.OpenSearchBackup.apply_post_restart_if_needed")
    @patch("charms.rolling_ops.v0.rollingops.RollingOpsManager._on_acquire_lock")
    @patch("charms.opensearch.v0.opensearch_plugin_manager.OpenSearchPluginManager._apply_config")
    @patch(
        "charms.opensearch.v0.opensearch_plugin_manager.OpenSearchPluginManager._install_if_needed"
    )
    def test_update_relation_data(
        self, mock_install, mock_apply_config, mock_acquire_lock, _, mock_status
    ) -> None:
        """Tests if new relation without data returns."""
        mock_status.return_value = PluginState.INSTALLED
        self.harness.update_relation_data(
            self.s3_rel_id,
            "s3-integrator",
            {
                "bucket": TEST_BUCKET_NAME,
                "access-key": "aaaa",
                "secret-key": "bbbb",
                "path": "/test",
                "endpoint": "localhost",
                "region": "testing-region",
                "storage-class": "storageclass",
            },
        )
        mock_install.assert_called_once()
        assert mock_apply_config.call_args[0][0].__dict__ == OpenSearchPluginConfig(
                config_entries_to_add={
                    **S3_OPENSEARCH_EXTRA_VALUES,
                    "s3.client.default.region": "testing-region",
                    "s3.client.default.endpoint": "localhost",
                },
                secret_entries_to_add={
                    "s3.client.default.access_key": "aaaa",
                    "s3.client.default.secret_key": "bbbb",
                },
            ).__dict__
        """
        mock_apply_config.assert_called_once_with(
            OpenSearchPluginConfig(
                config_entries_to_add={
                    **S3_OPENSEARCH_EXTRA_VALUES,
                    "s3.client.default.region": "testing-region",
                    "s3.client.default.endpoint": "localhost",
                },
                secret_entries_to_add={
                    "s3.client.default.access_key": "aaaa",
                    "s3.client.default.secret_key": "bbbb",
                },
            )
        )
        """
        mock_acquire_lock.assert_called_once()

    @patch("charms.opensearch.v0.opensearch_distro.OpenSearchDistribution.request")
    @patch("charms.opensearch.v0.opensearch_plugin_manager.OpenSearchPluginManager.status")
    def test_apply_post_restart_if_needed(self, mock_status, mock_request) -> None:
        """Tests the application of post-restart steps."""
        mock_request.side_effects = [  # list of returns for each call
            # 1st request: _service_already_registered
            # the request must contain an "error" to simulate the missing repo
            {"error": {"root_cause": [{"type": "repository_missing_exception"}]}},
            # 2nd request: _register_snapshot_repo
            # return success (i.e. no "error") to simulate a successful registration
            {},
        ]
        mock_status.return_value = PluginState.ENABLED
        self.charm.backup.apply_post_restart_if_needed()
        mock_request.called_once_with("GET", f"_snapshot/{OPENSEARCH_REPOSITORY_NAME}")
        mock_request.called_once_with(
            "PUT",
            f"_snapshot/{OPENSEARCH_REPOSITORY_NAME}",
            payload={
                "type": "s3",
                "settings": {
                    "bucket": TEST_BUCKET_NAME,
                    "base_path": S3_REPO_BASE_PATH,
                },
            },
        )

    @patch("charms.opensearch.v0.opensearch_backups.OpenSearchBackup.apply_post_restart_if_needed")
    @patch("charms.rolling_ops.v0.rollingops.RollingOpsManager._on_acquire_lock")
    @patch("charms.opensearch.v0.opensearch_plugin_manager.OpenSearchPluginManager._apply_config")
    @patch(
        "charms.opensearch.v0.opensearch_plugin_manager.OpenSearchPluginManager._install_if_needed"
    )
    @patch("charms.opensearch.v0.opensearch_distro.OpenSearchDistribution.request")
    @patch("charms.opensearch.v0.opensearch_backups.OpenSearchBackup._execute_s3_depart_calls")
    @patch("charms.opensearch.v0.opensearch_plugin_manager.OpenSearchPluginManager.status")
    def test_relation_departed(
        self,
        mock_status,
        mock_execute_s3_depart_calls,
        mock_request,
        mock_install,
        mock_apply_config,
        mock_acquire_lock,
        _,
    ) -> None:
        """Tests depart relation unit."""
        mock_request.side_effects = [  # list of returns for each call
            # 1st request: _check_snapshot_status
            # Return a response with SUCCESS in:
            {"SUCCESS"},
        ]
        mock_status.return_value = PluginState.ENABLED
        self.harness.remove_relation_unit(self.s3_rel_id, "s3-integrator/0")

        mock_request.called_once_with("GET", "/_snapshot/_status")
        mock_execute_s3_depart_calls.assert_called_once()
        # As plugin_manager's run() is called, then so install(), config() and disable():
        mock_install.assert_called_once()
        assert mock_apply_config.call_args[0][0].__dict__ == OpenSearchPluginConfig(
                config_entries_to_del=[
                    *(S3_OPENSEARCH_EXTRA_VALUES.keys()),
                    "s3.client.default.region",
                    "s3.client.default.endpoint",
                ],
                secret_entries_to_del=[
                    "s3.client.default.access_key",
                    "s3.client.default.secret_key",
                ],
            ).__dict__
        """
        mock_apply_config.assert_called_once_with(
            OpenSearchPluginConfig(
                config_entries_to_del=[
                    *(S3_OPENSEARCH_EXTRA_VALUES.keys()),
                    "s3.client.default.region",
                    "s3.client.default.endpoint",
                ],
                secret_entries_to_del=[
                    "s3.client.default.access_key",
                    "s3.client.default.secret_key",
                ],
            )
        )
        """
        mock_acquire_lock.assert_called_once()
