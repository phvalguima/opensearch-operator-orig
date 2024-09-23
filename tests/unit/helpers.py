# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import responses
from ops.model import Model, Unit

NODE_ID = "yTLtw5wNQlCsHUcrKaU5Kw"
CLUSTER_NAME = "opensearch-nfp7"


def mock_deployment_desc(
    model_uuid: str,
    roles: list[str],
    state: str,
    typ: str,
    temperature: str | None = None,
    cluster_name: str = CLUSTER_NAME,
) -> dict[str, str]:
    return {
        "app": {
            "id": f"{model_uuid}/opensearch",
            "model_uuid": model_uuid,
            "name": "opensearch",
            "short_id": "5a5",
        },
        "config": {
            "cluster_name": cluster_name,
            "data_temperature": temperature,
            "init_hold": False,
            "roles": roles,
        },
        "pending_directives": [],
        "promotion_time": 1721391694.387948,
        "start": "start-with-generated-roles",
        "state": {"message": "", "value": "active"},
        "typ": typ,
    }


def mock_response_root(unit_name: str, host: str, cluster_name: str = CLUSTER_NAME):
    """Add API mock for the API root ('/') query.

    Keep in mind to add @responses.activate decorator to the test function using this call!
    """
    expected_response_root = {
        "name": unit_name.replace("/", "-"),
        "cluster_name": cluster_name,
        "cluster_uuid": "TYji6UEuSw2tnIL-z8xEOg",
        "version": {
            "distribution": "opensearch",
            "number": "2.14.0",
            "build_type": "tar",
            "build_hash": "30dd870855093c9dca23fc6f8cfd5c0d7c83127d",
            "build_date": "2024-08-05T16:00:25.471849593Z",
            "build_snapshot": False,
            "lucene_version": "9.10.0",
            "minimum_wire_compatibility_version": "7.10.0",
            "minimum_index_compatibility_version": "7.0.0",
        },
        "tagline": "The OpenSearch Project: https://opensearch.org/",
    }

    responses.add(
        method="GET",
        url=f"https://{host}:9200/",
        json=expected_response_root,
        status=200,
    )


def mock_response_nodes(
    unit_name: str, host: str, node_id: str = NODE_ID, cluster_name: str = CLUSTER_NAME
):
    """Add API mock for the API ('/nodes') query.

    Keep in mind to add @responses.activate decorator to the test function using this call!
    NOTE: unit_name should be charm.unit_name (NOT charm.unit.name)
    """
    expected_response_nodes = {
        "_nodes": {"total": 1, "successful": 1, "failed": 0},
        "cluster_name": cluster_name,
        "nodes": {
            node_id: {
                "name": unit_name.replace("/", "-"),
                "transport_address": f"{host}:9300",
                "host": host,
                "ip": host,
                "version": "2.14.0",
                "build_type": "tar",
                "build_hash": "30dd870855093c9dca23fc6f8cfd5c0d7c83127d",
                "total_indexing_buffer": 107374182,
                "roles": ["cluster_manager", "data", "ingest", "ml"],
                "attributes": {
                    "shard_indexing_pressure_enabled": "true",
                    "app_id": "617e5f02-5be5-4e25-85f0-276b2347a5ad/opensearch",
                },
            },
        },
    }

    responses.add(
        method="GET", url=f"https://{host}:9200/_nodes", json=expected_response_nodes, status=200
    )


def mock_response_mynode(
    unit_name: str, host: str, node_id: str = NODE_ID, cluster_name: str = CLUSTER_NAME
):
    """Add API mock for the API root ('/') query.

    Keep in mind to add @responses.activate decorator to the test function using this call!
    """
    expected_response_mynode = {
        "_nodes": {"total": 1, "successful": 1, "failed": 0},
        "cluster_name": cluster_name,
        "nodes": {
            node_id: {
                "name": unit_name,
                "transport_address": f"{host}:9300",
                "host": host,
                "ip": host,
                "version": "2.14.0",
                "build_type": "tar",
                "build_hash": "30dd870855093c9dca23fc6f8cfd5c0d7c83127d",
                "total_indexing_buffer": 107374182,
                "roles": ["cluster_manager", "data", "ingest", "ml"],
                "attributes": {
                    "shard_indexing_pressure_enabled": "true",
                    "app_id": "617e5f02-5be5-4e25-85f0-276b2347a5ad/opensearch",
                },
                "settings": {
                    "cluster": {
                        "name": "opensearch-nfp7",
                        "initial_cluster_manager_nodes": ["opensearch-1.e74"],
                    },
                    "node": {
                        "attr": {
                            "app_id": "617e5f02-5be5-4e25-85f0-276b2347a5ad/opensearch",
                            "shard_indexing_pressure_enabled": "true",
                        },
                        "name": unit_name,
                        "roles": [
                            "data",
                            "ingest",
                            "ml",
                            "cluster_manager",
                        ],
                    },
                    "path": {
                        "data": ["/var/snap/opensearch/common/var/lib/opensearch"],
                        "logs": "/var/snap/opensearch/common/var/log/opensearch",
                        "home": "/var/snap/opensearch/current/usr/share/opensearch",
                    },
                    "discovery": {"seed_providers": "file"},
                    "client": {"type": "node"},
                    "http": {
                        "compression": "false",
                        "type": "org.opensearch.security.http.SecurityHttpServerTransport",
                        "type.default": "netty4",
                    },
                    "index": {},
                },
            }
        },
    }
    responses.add(
        method="GET",
        url=f"https://{host}:9200/_nodes/{node_id}",
        json=expected_response_mynode,
        status=200,
    )


def get_relation_unit(model: Model, relation_name: str, unit_name: str) -> Unit | None:
    """Get the Unit object from the relation that matches unit_name."""
    relation = model.get_relation(relation_name)
    if not relation.units:
        return

    for unit in relation.units:
        if unit.name == unit_name:
            return unit