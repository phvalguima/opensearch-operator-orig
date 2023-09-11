# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Implements the KNN and ML-Commons plugins for OpenSearch."""

import logging
from typing import Any, Dict

from charms.opensearch.v0.opensearch_plugins import OpenSearchPlugin

logger = logging.getLogger(__name__)


# The unique Charmhub library identifier, never change it
LIBID = "71166db20ab244099ae966c8055db2df"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1


class OpenSearchKnn(OpenSearchPlugin):
    """Implements the opensearch-knn plugin."""

    def __init__(
        self,
        plugins_path: str,
    ):
        super().__init__(plugins_path)

    def config(self) -> Dict[str, Dict[str, str]]:
        """Returns a dict containing all the configuration needed to be applied in the form.

        Format:
        {
            "opensearch": {...},
            "keystore": {...},
        }
        """
        return {"opensearch": {"knn.plugin.enabled": "true"}, "keystore": {}}

    def _is_enabled(self, opensearch_config: Dict[str, Any]) -> None:
        self._enabled = opensearch_config.get("knn.plugin.enabled", "false") == "true"

    def disable(self) -> Dict[str, Any]:
        """Returns a dict containing different config changes.

        The dict should return:
        (1) all the configs to remove
        (2) all the configs to add
        (3) all the keystore values to be remmoved.
        """
        return {
            "to_remove_opensearch": ["knn.plugin.enabled"],
            "to_add_opensearch": [],
            "to_remove_keystore": [],
        }

    @property
    def name(self) -> str:
        """Returns the name of the plugin."""
        return "opensearch-knn"
