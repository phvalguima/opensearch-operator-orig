# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

"""In this class we manage certificates relation.

This class handles certificate request and renewal through
the interaction with the TLS Certificates Operator.

This library needs https://charmhub.io/tls-certificates-interface/libraries/tls_certificates
library is imported to work.

It requires a charm that extends OpenSearchBaseCharm as it refers internal objects of that class.
— update_config: to disable TLS when relation with the TLS Certificates Operator is broken.
"""

import base64
import logging
import re
import socket
from typing import Dict, List, Optional, Tuple

from charms.opensearch.v0.helpers.databag import Scope
from charms.opensearch.v0.helpers.networking import get_host_ip, get_hostname_by_unit
from charms.opensearch.v0.opensearch_distro import OpenSearchError
from charms.opensearch.v0.tls_constants import TLS_RELATION, CertType
from charms.tls_certificates_interface.v1.tls_certificates import (
    CertificateAvailableEvent,
    CertificateExpiringEvent,
    TLSCertificatesRequiresV1,
    generate_csr,
    generate_private_key,
)
from ops.charm import ActionEvent, RelationBrokenEvent, RelationJoinedEvent
from ops.framework import Object

# The unique Charmhub library identifier, never change it
LIBID = "f4bd9c1dad554f9ea52954b8181cdc19"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

logger = logging.getLogger(__name__)


class OpenSearchTLS(Object):
    """Class that Manages OpenSearch relation with TLS Certificates Operator."""

    def __init__(self, charm, peer_relation: str):
        super().__init__(charm, "client-relations")

        self.charm = charm
        self.peer_relation = peer_relation
        self.certs = TLSCertificatesRequiresV1(charm, TLS_RELATION)

        self.framework.observe(
            self.charm.on.set_tls_private_key_action, self._on_set_tls_private_key
        )

        self.framework.observe(
            self.charm.on[TLS_RELATION].relation_joined, self._on_tls_relation_joined
        )
        self.framework.observe(
            self.charm.on[TLS_RELATION].relation_broken, self._on_tls_relation_broken
        )

        self.framework.observe(self.certs.on.certificate_available, self._on_certificate_available)
        self.framework.observe(self.certs.on.certificate_expiring, self._on_certificate_expiring)

    def _on_set_tls_private_key(self, event: ActionEvent) -> None:
        """Set the TLS private key, which will be used for requesting the certificate."""
        cert_type = CertType(event.params["category"])  # type
        scope = Scope.APP if cert_type == CertType.APP_ADMIN else Scope.UNIT

        if scope == Scope.APP and not self.charm.unit.is_leader():
            event.log("Only the juju leader unit can set private key for the admin certificates.")
            return

        try:
            self._request_certificate(
                scope, cert_type, event.params.get("key", None), event.params.get("password", None)
            )
        except ValueError as e:
            event.fail(str(e))

    def _on_tls_relation_joined(self, _: RelationJoinedEvent) -> None:
        """Request certificate when TLS relation joined."""
        if self.charm.unit.is_leader():
            self._request_certificate(Scope.APP, CertType.APP_ADMIN)

        self._request_certificate(Scope.UNIT, CertType.UNIT_TRANSPORT)
        self._request_certificate(Scope.UNIT, CertType.UNIT_HTTP)

    def _on_tls_relation_broken(self, _: RelationBrokenEvent) -> None:
        """We don't need to handle much here."""
        pass

    def _on_certificate_available(self, event: CertificateAvailableEvent) -> None:
        """Enable TLS when TLS certificate available."""
        try:
            scope, cert_type, secrets = self._find_secret(event.certificate_signing_request, "csr")
            logger.debug(f"{scope.value}.{cert_type.value} TLS certificate available.")
        except TypeError:
            logger.debug("Unknown certificate available.")
            return

        old_cert = secrets["cert"]
        renewal = old_cert and old_cert != event.certificate

        self.charm.secrets.put_object(
            scope,
            cert_type.value,
            {
                "chain": event.chain,
                "cert": event.certificate,
                "ca": event.ca,
            },
            merge=True,
        )

        try:
            self.charm.on_tls_conf_set(scope, cert_type, renewal)
        except OpenSearchError:
            event.defer()

    def _on_certificate_expiring(self, event: CertificateExpiringEvent) -> None:
        """Request the new certificate when old certificate is expiring."""
        try:
            scope, cert_type, secrets = self._find_secret(event.certificate, "cert")
            logger.debug(f"{scope.value}.{cert_type.value} TLS certificate expiring.")
        except TypeError:
            logger.debug("Unknown certificate expiring.")
            return

        key = secrets["key"].encode("utf-8")
        key_password = secrets.get("key-password", None)
        old_csr = secrets["csr"].encode("utf-8")

        subject = self._get_subject(cert_type)
        new_csr = generate_csr(
            private_key=key,
            private_key_password=(None if key_password is None else key_password.encode("utf-8")),
            subject=subject,
            organization=self.charm.app.name,
            sans=self._get_sans(cert_type),
        )

        self.charm.secrets.put_object(
            scope, cert_type, {"csr": new_csr.decode("utf-8"), "subject": subject}, merge=True
        )

        self.certs.request_certificate_renewal(
            old_certificate_signing_request=old_csr,
            new_certificate_signing_request=new_csr,
        )

    def _request_certificate(
        self,
        scope: Scope,
        cert_type: CertType,
        param: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """Request certificate and store the key/key-password/csr in the scope's data bag."""
        if param is None:
            key = generate_private_key()
        else:
            key = self._parse_tls_file(param)

        if password is not None:
            password = password.encode("utf-8")

        subject = self._get_subject(cert_type)
        csr = generate_csr(
            private_key=key,
            private_key_password=password,
            subject=subject,
            organization=self.charm.app.name,
            sans=self._get_sans(cert_type),
        )

        self.charm.secrets.put_object(
            scope,
            cert_type.value,
            {
                "key": key.decode("utf-8"),
                "key-password": password,
                "csr": csr.decode("utf-8"),
                "subject": subject,
            },
            merge=True,
        )

        if self.charm.model.get_relation(TLS_RELATION):
            self.certs.request_certificate_creation(certificate_signing_request=csr)

    def _get_sans(self, cert_type: CertType) -> Optional[List[str]]:
        """Create a list of DNS names for an OpenSearch unit.

        Returns:
            A list representing the hostnames of the OpenSearch unit.
            or None if admin cert_type, because that cert is not tied to a specific host.
        """
        if cert_type == CertType.APP_ADMIN:
            return None

        unit_id = self.charm.unit.name.split("/")[1]

        return [
            f"{self.charm.app.name}-{unit_id}",
            socket.getfqdn(),
            get_host_ip(self.charm, self.peer_relation),
        ]

    def _get_subject(self, cert_type: CertType) -> str:
        if cert_type == CertType.APP_ADMIN:
            return "admin"

        return get_hostname_by_unit(self.charm, self.charm.unit.name)

    @staticmethod
    def _parse_tls_file(raw_content: str) -> bytes:
        """Parse TLS files from both plain text or base64 format."""
        if re.match(r"(-+(BEGIN|END) [A-Z ]+-+)", raw_content):
            return re.sub(
                r"(-+(BEGIN|END) [A-Z ]+-+)",
                "\n\\1\n",
                raw_content,
            ).encode("utf-8")
        return base64.b64decode(raw_content)

    def _find_secret(
        self, event_data: str, secret_name: str
    ) -> Optional[Tuple[Scope, CertType, Dict[str, str]]]:
        """Find secret across all scopes (app, unit) and across all cert types.

        Returns:
            scope: scope type of the secret.
            cert type: certificate type of the secret (APP_ADMIN, UNIT_HTTP etc.)
            secret: dictionary of the data stored in this secret
        """

        def is_secret_found(secrets: Optional[Dict[str, str]]) -> bool:
            return (
                secrets is not None
                and secrets.get(secret_name, "").rstrip() == event_data.rstrip()
            )

        app_secrets = self.charm.secrets.get_object(Scope.APP, CertType.APP_ADMIN)
        if is_secret_found(app_secrets):
            return Scope.APP, CertType.APP_ADMIN, app_secrets

        u_transport_secrets = self.charm.secrets.get_object(Scope.UNIT, CertType.UNIT_TRANSPORT)
        if is_secret_found(u_transport_secrets):
            return Scope.UNIT, CertType.UNIT_TRANSPORT, u_transport_secrets

        u_http_secrets = self.charm.secrets.get_object(Scope.UNIT, CertType.UNIT_HTTP)
        if is_secret_found(u_http_secrets):
            return Scope.UNIT, CertType.UNIT_HTTP, u_http_secrets

        return None
