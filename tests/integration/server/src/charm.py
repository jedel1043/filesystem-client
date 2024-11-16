#!/usr/bin/env python3
# Copyright 2024 Canonical
# See LICENSE file for licensing details.

"""Charm the application."""

import logging
import pathlib
import subprocess

import ops


_logger = logging.getLogger(__name__)


class StorageServerCharm(ops.CharmBase):
    def __init__(self, framework: ops.Framework):
        super().__init__(framework)
        framework.observe(self.on.start, self._on_start)
        framework.observe(
            self.on["fs-share"].relation_joined, self._on_relation_joined
        )
        framework.observe(
            self.on["fs-share"].relation_changed, self._on_relation_joined
        )

    def _on_start(self, event: ops.StartEvent):
        """Handle start event."""
        self.unit.status = ops.ActiveStatus()

    def _on_relation_joined(self, event: ops.RelationJoinedEvent) -> None:
        event.relation.data[self.app].update({"data": "ceph://fsuser@(192.168.1.1,192.168.1.2,192.168.1.3)/data?fsid=asdf1234&auth-info=QWERTY1234&filesystem=fs_name"})

if __name__ == "__main__":  # pragma: nocover
    ops.main(StorageServerCharm)  # type: ignore