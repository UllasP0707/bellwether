"""Source integrations.

Adding a source means a new `Connector` subclass with `fetch` and `parse`, and
one line in `CONNECTORS`. Everything else — archival, identity resolution,
deterministic event ids, cursors, counters — comes from the base class.
"""

from bellwether.connectors.base import (
    Connector,
    EmployeeDirectory,
    Page,
    ParsedRecord,
    PollResult,
    deterministic_event_id,
)
from bellwether.connectors.email_gateway import EmailGatewayConnector
from bellwether.connectors.endpoint_agent import EndpointAgentConnector
from bellwether.connectors.google_workspace import GoogleWorkspaceConnector
from bellwether.connectors.http import ConnectorError, VendorClient
from bellwether.connectors.okta import OktaConnector

CONNECTORS: dict[str, type[Connector]] = {
    OktaConnector.name: OktaConnector,
    GoogleWorkspaceConnector.name: GoogleWorkspaceConnector,
    EmailGatewayConnector.name: EmailGatewayConnector,
    EndpointAgentConnector.name: EndpointAgentConnector,
}

__all__ = [
    "CONNECTORS",
    "Connector",
    "ConnectorError",
    "EmailGatewayConnector",
    "EmployeeDirectory",
    "EndpointAgentConnector",
    "GoogleWorkspaceConnector",
    "OktaConnector",
    "Page",
    "ParsedRecord",
    "PollResult",
    "VendorClient",
    "deterministic_event_id",
]
