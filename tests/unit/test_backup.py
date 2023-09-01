# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit test for the opensearch_backup library."""
import unittest
from unittest.mock import MagicMock, call, patch

from charms.opensearch.v0.constants_charm import S3_RELATION, PeerRelationName
from charms.opensearch.v0.constants_tls import TLS_RELATION
from charms.opensearch.v0.helper_conf_setter import YamlConfigSetter
from charms.opensearch.v0.opensearch_backup import OpenSearchBackup
from charms.rolling_ops.v0.rollingops import RollingOpsManager
from ops.testing import Harness

from charm import OpenSearchOperatorCharm


class TestOpenSearchBackup(unittest.TestCase):
    BASE_LIB_PATH = "charms.opensearch.v0"
    BASE_CHARM_CLASS = f"{BASE_LIB_PATH}.opensearch_base_charm.OpenSearchBaseCharm"

    def setUp(self) -> None:
        self.harness = Harness(OpenSearchOperatorCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.set_leader(is_leader=True)

        self.harness.begin()
        self.charm = self.harness.charm
        self.harness.add_relation(PeerRelationName, self.charm.app.name)
        self.harness.add_relation(TLS_RELATION, self.charm.app.name)
        # self.s3_rel = self.harness.add_relation(S3_RELATION, self.charm.app.name)

        self.secret_store = self.charm.secrets

    @patch.object(OpenSearchBackup, "_request")
    @patch.object(OpenSearchBackup, "register_snapshot_repo")
    @patch.object(OpenSearchBackup, "_is_started")
    @patch.object(RollingOpsManager, "_on_acquire_lock")
    @patch.object(YamlConfigSetter, "put")
    def test_add_s3_data_relation(
        self,
        mock_yaml_setter_put,
        mock_acquire_lock,
        mock_started,
        mock_register_repo,
        mock_request,
    ):
        """Test the add relation and its access to plugin, keystore, config."""
        self.charm.opensearch.add_plugin_without_restart = MagicMock()
        mock_add_plugin = self.charm.opensearch.add_plugin_without_restart
        self.charm.opensearch.add_to_keystore = MagicMock()
        mock_add_ks = self.charm.opensearch.add_to_keystore
        self.charm.opensearch.list_plugins = MagicMock(return_value=[])

        mock_started.return_value = True
        mock_register_repo.return_value = False
        mock_request.return_value = {"status": 200}

        s3_integrator_id = self.harness.add_relation(S3_RELATION, "s3-integrator")
        self.harness.add_relation_unit(s3_integrator_id, "s3-integrator/0")
        self.harness.update_relation_data(
            s3_integrator_id,
            "s3-integrator",
            {
                "bucket": "s3://unit-test",
                "access-key": "aaaa",
                "secret-key": "bbbb",
                "path": "/test",
                "endpoint": "localhost",
                "region": "testing-region",
                "storage-class": "storageclass",
            },
        )

        mock_add_plugin.assert_called_once_with("repository-s3")
        mock_acquire_lock.assert_called()
        yaml_setter_put_calls = [
            call("opensearch.yml", "s3.client.default.endpoint", "localhost"),
            call("opensearch.yml", "s3.client.default.region", "testing-region"),
            call("opensearch.yml", "s3.client.default.max_retries", 3),
            call("opensearch.yml", "s3.client.default.path_style_access", False),
            call("opensearch.yml", "s3.client.default.protocol", "https"),
            call("opensearch.yml", "s3.client.default.read_timeout", "50s"),
            call("opensearch.yml", "s3.client.default.use_throttle_retries", True),
        ]
        for c in yaml_setter_put_calls:
            assert c in mock_yaml_setter_put.call_args_list
        # mock_yaml_setter_put.assert_has_calls(yaml_setter_put_calls)
        mock_add_ks.assert_has_calls(
            [
                call("s3.client.default.access_key", "aaaa"),
                call("s3.client.default.secret_key", "bbbb"),
            ]
        )
        mock_request.assert_has_calls([call("POST", "_nodes/reload_secure_settings")])
