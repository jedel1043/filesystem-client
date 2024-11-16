# Copyright 2024 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
"""

import json
import logging
from dataclasses import asdict, dataclass
from typing import Dict, Iterable, List, Optional, Set
from enum import Enum
import urllib

import ops
from ops.charm import (
    CharmBase,
    CharmEvents,
    RelationChangedEvent,
    RelationDepartedEvent,
    RelationEvent,
    RelationJoinedEvent,
)
from ops.framework import EventSource, Object
from ops.model import Relation, SecretNotFoundError

# The unique Charmhub library identifier, never change it
LIBID = ""

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1

_logger = logging.getLogger(__name__)

class FsInterfacesError(Exception):
    """Exception raised when a storage operation failed."""

    @property
    def message(self) -> str:
        """Return message passed as argument to exception."""
        return self.args[0]

class FsType(Enum):
    FS = "fs"
    NFS = "nfs"
    LUSTREFS = "lustrefs"



class ServerConnectedEvent(RelationEvent):
    """Emit when a FS server is integrated with FS client."""


@dataclass(init=False)
class Endpoint:
    fs_type: FsType
    user: str
    hosts: [str]
    path: str
    options: Dict[str, str]

    def __init__(self, uri: str):
        _logger.debug(f"parsing `{uri}` to Endpoint")

        uri = urllib.parse.urlparse(uri, allow_fragments=False)
        scheme = str(uri.scheme)
        try:
            self.fs_type = FsType(scheme)
        except ValueError:
            raise FsInterfacesError("invalid schema for endpoint")
        
        user = urllib.parse.unquote(uri.username)
        hostname = urllib.parse.unquote(uri.hostname)
        if not hostname or hostname[0] != '(' or hostname[-1] != ')':
            _logger.debug(f"parsing failed for hostname `{hostname}`")
            raise FsInterfacesError("invalid list of hosts for endpoint")
        self.hosts = hostname.split(',')
        self.path = uri.path
        try:
            self.options = urllib.parse.parse_qs(uri.query, strict_parsing=True)
        except ValueError:
            _logger.debug(f"parsing failed for query `{uri.query}`")
            raise FsInterfacesError("invalid options for endpoint")


class _MountEvent(RelationEvent):
    """Base event for mount-related events."""

    @property
    def endpoint(self) -> Optional[Endpoint]:
        """Get endpoint info."""
        if not (uri := self.relation.data[self.relation.app].get("endpoint")):
            return
        return Endpoint(uri)


class MountShareEvent(_MountEvent):
    """Emit when FS share is ready to be mounted."""


class UmountShareEvent(_MountEvent):
    """Emit when FS share needs to be unmounted."""


class _FSRequiresEvents(CharmEvents):
    """Events that FS servers can emit."""

    server_connected = EventSource(ServerConnectedEvent)
    mount_share = EventSource(MountShareEvent)
    umount_share = EventSource(UmountShareEvent)


class ShareRequestedEvent(RelationEvent):
    """Emit when a consumer requests a new FS share be created by the provider."""

    @property
    def name(self) -> Optional[str]:
        """Get name of requested FS share."""
        return self.relation.data[self.relation.app].get("name")


class _FSProvidesEvents(CharmEvents):
    """Events that FS clients can emit."""

    share_requested = EventSource(ShareRequestedEvent)


class _BaseInterface(Object):
    """Base methods required for FS share integration interfaces."""

    def __init__(self, charm: CharmBase, integration_name) -> None:
        super().__init__(charm, integration_name)
        self.charm = charm
        self.app = charm.model.app
        self.unit = charm.unit
        self.integration_name = integration_name

    @property
    def integrations(self) -> List[Relation]:
        """Get list of active integrations associated with the integration name."""
        result = []
        for integration in self.charm.model.relations[self.integration_name]:
            try:
                _ = repr(integration.data)
                result.append(integration)
            except RuntimeError:
                pass
        return result

    def fetch_data(self) -> Dict:
        """Fetch integration data.

        Notes:
            Method cannot be used in `*-relation-broken` events and will raise an exception.

        Returns:
            Dict:
                Values stored in the integration data bag for all integration instances.
                Values are indexed by the integration ID.
        """
        result = {}
        for integration in self.integrations:
            result[integration.id] = {
                k: v for k, v in integration.data[integration.app].items() if k != "cache"
            }
        return result

    def _update_data(self, integration_id: int, data: Dict) -> None:
        """Update a set of key-value pairs in integration.

        Args:
            integration_id: Identifier of particular integration.
            data: Key-value pairs that should be updated in integration data bucket.

        Notes:
            Only the application leader unit can update the
            integration data bucket using this method.
        """
        if self.unit.is_leader():
            integration = self.charm.model.get_relation(self.integration_name, integration_id)
            integration.data[self.app].update(data)


class FSRequires(_BaseInterface):
    """Consumer-side interface of FS share integrations."""

    on = _FSRequiresEvents()

    def __init__(self, charm: CharmBase, integration_name: str) -> None:
        super().__init__(charm, integration_name)
        self.framework.observe(
            charm.on[integration_name].relation_joined, self._on_relation_joined
        )
        self.framework.observe(
            charm.on[integration_name].relation_changed, self._on_relation_changed
        )
        self.framework.observe(
            charm.on[integration_name].relation_departed, self._on_relation_departed
        )

    def _on_relation_joined(self, event: RelationJoinedEvent) -> None:
        """Handle when client and server are first integrated."""
        if self.unit.is_leader():
            _logger.debug("Emitting `ServerConnected` event from `RelationJoined` hook")
            self.on.server_connected.emit(event.relation, app=event.app, unit=event.unit)

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Handle when the databag between client and server has been updated."""
        transaction = _eval(event, self.unit)

        if (
            "endpoint" in transaction.changed
        ):
            _logger.debug("Emitting `MountShare` event from `RelationChanged` hook")
            self.on.mount_share.emit(event.relation, app=event.app, unit=event.unit)

    def _on_relation_departed(self, event: RelationDepartedEvent) -> None:
        """Handle when server departs integration."""
        _logger.debug("Emitting `UmountShare` event from `RelationDeparted` hook")
        self.on.umount_share.emit(event.relation, app=event.app, unit=event.unit)

    def request_share(
        self,
        integration_id: int,
        name: str,
    ) -> None:
        """Request access to a FS share.

        Args:
            integration_id: Identifier for specific integration.
            name: Name of the FS share.

        Notes:
            Only application leader unit can request a FS share.
        """
        if self.unit.is_leader():
            params = {"name": name}
            _logger.debug(f"Requesting FS share with parameters {params}")
            self._update_data(integration_id, params)


class FSProvides(_BaseInterface):
    """Provider-side interface of FS share integrations."""

    on = _FSProvidesEvents()

    def __init__(self, charm: CharmBase, integration_name: str) -> None:
        super().__init__(charm, integration_name)
        self.framework.observe(
            charm.on[integration_name].relation_changed, self._on_relation_changed
        )
        self.framework.observe(charm.on.secret_remove, self._on_secret_remove)

    def _on_secret_remove(self, event: ops.SecretRemoveEvent):
        """Remove revisions that are no longer tracked by any observer."""
        event.secret.remove_revision(event.revision)

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Handle when the databag between client and server has been updated."""
        if self.unit.is_leader():
            transaction = _eval(event, self.unit)
            if "name" in transaction.added:
                _logger.debug("Emitting `RequestShare` event from `RelationChanged` hook")
                self.on.share_requested.emit(event.relation, app=event.app, unit=event.unit)

    def set_share(
        self, integration_id: int, share_info: Endpoint, auth_info: FSAuthInfo
    ) -> None:
        """Set info for mounting a FS share.

        Args:
            integration_id: Identifier for specific integration.
            share_info: Information required to mount the FS share.
            auth_info: Information required to authenticate against the  cluster.

        Notes:
            Only the application leader unit can set the FS share data.
        """
        if self.unit.is_leader():
            share_info = json.dumps(asdict(share_info))
            auth_info = asdict(auth_info)
            _logger.debug(f"Exporting FS share with info {share_info}")

            try:
                secret = self.model.get_secret(label="auth_info")
                secret.set_content(auth_info)
                secret.get_content(refresh=True)
            except SecretNotFoundError:
                secret = self.app.add_secret(
                    auth_info,
                    label="auth_info",
                    description="Auth info to authenticate against the FS share",
                )

            integration = self.charm.model.get_relation(self.integration_name, integration_id)
            secret.grant(integration)
            self._update_data(
                integration_id,
                {
                    "share_info": share_info,
                    "auth": secret.id,
                    "auth-rev": str(secret.get_info().revision),
                },
            )

@dataclass(frozen=True)
class _Transaction:
    """Store transaction information between to data mappings."""

    added: Set
    changed: Set
    deleted: Set


def _eval(event: RelationChangedEvent, bucket: str) -> _Transaction:
    """Evaluate the difference between data in an integration changed databag.

    Args:
        event: Integration changed event.
        bucket: Bucket of the databag. Can be application or unit databag.

    Returns:
        _Transaction:
            Transaction info containing the added, deleted, and changed
            keys from the event integration databag.
    """
    # Retrieve the old data from the data key in the application integration databag.
    old_data = json.loads(event.relation.data[bucket].get("cache", "{}"))
    # Retrieve the new data from the event integration databag.
    new_data = {
        key: value for key, value in event.relation.data[event.app].items() if key != "cache"
    }
    # These are the keys that were added to the databag and triggered this event.
    added = new_data.keys() - old_data.keys()
    # These are the keys that were removed from the databag and triggered this event.
    deleted = old_data.keys() - new_data.keys()
    # These are the keys that were added or already existed in the databag, but had their values changed.
    changed = added.union(
        {key for key in old_data.keys() & new_data.keys() if old_data[key] != new_data[key]}
    )
    # Convert the new_data to a serializable format and save it for a next diff check.
    event.relation.data[bucket].update({"cache": json.dumps(new_data)})

    # Return the transaction with all possible changes.
    return _Transaction(added, changed, deleted)
