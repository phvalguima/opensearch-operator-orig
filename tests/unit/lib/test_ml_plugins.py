# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit test for the opensearch_plugins library."""
import unittest
from unittest.mock import MagicMock, PropertyMock, call, patch

import charms
from charms.opensearch.v0.opensearch_ml_plugins import OpenSearchKnn
from charms.opensearch.v0.opensearch_plugins import PluginState
from charms.rolling_ops.v0.rollingops import RollingOpsManager
from ops.testing import Harness

from charm import OpenSearchOperatorCharm

RETURN_LIST_PLUGINS = """opensearch-alerting
opensearch-anomaly-detection
opensearch-asynchronous-search
opensearch-cross-cluster-replication
opensearch-geospatial
opensearch-index-management
opensearch-job-scheduler
opensearch-knn
opensearch-ml
opensearch-notifications
opensearch-notifications-core
opensearch-observability
opensearch-performance-analyzer
opensearch-reports-scheduler
opensearch-security
opensearch-sql
"""


class TestOpenSearchKNN(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = Harness(OpenSearchOperatorCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()
        self.charm = self.harness.charm
        self.charm.opensearch.paths.plugins = "tests/unit/resources"
        self.plugin_manager = self.charm.plugin_manager
        self.plugin_manager._plugins_path = self.charm.opensearch.paths.plugins
        # Override the ConfigExposedPlugins and ensure one single plugin exists
        charms.opensearch.v0.opensearch_plugin_manager.ConfigExposedPlugins = {
            "opensearch-knn": {
                "class": OpenSearchKnn,
                "config-name": "plugin_opensearch_knn",
                "relation-name": None,
            }
        }

    @patch.object(RollingOpsManager, "_on_acquire_lock")
    @patch(
        "charms.opensearch.v0.opensearch_distro.OpenSearchDistribution.version",
        new_callable=PropertyMock,
    )
    @patch("charms.opensearch.v0.opensearch_plugin_manager.OpenSearchPluginManager._is_enabled")
    @patch("charms.opensearch.v0.opensearch_plugin_manager.OpenSearchPluginManager.status")
    @patch("charms.opensearch.v0.opensearch_distro.OpenSearchDistribution.is_started")
    @patch("charms.opensearch.v0.opensearch_config.OpenSearchConfig.load_node")
    @patch("charms.opensearch.v0.helper_conf_setter.YamlConfigSetter.put")
    def test_config_changed(
        self,
        _,
        mock_load,
        mock_is_started,
        mock_status,
        mock_is_enabled,
        mock_version,
        mock_acquire_lock,
    ) -> None:
        """Tests entire config_changed event with KNN plugin."""
        mock_status.side_effect = [PluginState.AVAILABLE, PluginState.ENABLED]
        mock_is_enabled.return_value = False
        mock_is_started.return_value = True
        mock_version.return_value = "2.9.0"
        self.plugin_manager._keystore.add = MagicMock()
        self.plugin_manager._add_plugin = MagicMock()
        self.charm.opensearch.run_bin = MagicMock(return_value=RETURN_LIST_PLUGINS)
        # Called 3x times
        mock_load.side_effect = [
            {},  # called at update_if_needed
            {},  # called at the beginning of plugin_manager.run() method
            {"knn.plugin.enabled": "true"},  # called at the end of run() method
        ]

        self.harness.update_config({"plugin_opensearch_knn": True})
        self.charm.opensearch.config.put.assert_has_calls(
            [call("opensearch.yml", "knn.plugin.enabled", "true")]
        )
        self.charm.opensearch.run_bin.assert_called_once_with("opensearch-plugin", "list")
        self.plugin_manager._add_plugin.assert_not_called()
        mock_acquire_lock.assert_called_once()
