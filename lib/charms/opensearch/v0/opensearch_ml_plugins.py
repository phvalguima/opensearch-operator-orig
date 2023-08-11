# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Implements the KNN and ML-Commons plugins for OpenSearch."""

import logging
from typing import Optional

from charms.opensearch.v0.opensearch_plugins import OpenSearchPlugin
from ops.framework import Object


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

    def __init__(self, name: str, charm: Object, relname: Optional[str]):
        super().__init__(charm, relname)

    def upgrade(self, uri: str) -> None:
        """Runs the upgrade process in this plugin."""
        # TODO: Not implemented yet
        return

    def is_enabled(self) -> bool:
        """Returns True if the plugin is enabled."""
        return True if self.distro.config.load(self.CONFIG_YML).get(
            "knn.plugin.enabled", "false") == "true" else False

    def disable(self) -> bool:
        """Disables the plugin."""
        return self.configure(opensearch_yml={"knn.plugin.enabled": "false"})

    def enable(self) -> bool:
        """Enables the plugin."""
        return self.configure(opensearch_yml={"knn.plugin.enabled": "true"})
