#!/usr/bin/env python3
# Copyright 2024 Jose Julian Espina
# See LICENSE file for licensing details.

import asyncio
import logging
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./charmcraft.yaml").read_text())
APP_NAME = METADATA["name"]


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    # Build and deploy charm from local source folder
    charm = await ops_test.build_charm(".")
    server = await ops_test.build_charm("./tests/integration/server")

    # Deploy the charm and wait for active/idle status
    await asyncio.gather(
        ops_test.model.deploy(charm, application_name=APP_NAME),
        ops_test.model.deploy(server, application_name="server1"),
        ops_test.model.deploy(server, application_name="server2"),
        ops_test.model.wait_for_idle(
            apps=[APP_NAME, "server1", "server2"], status="active", raise_on_blocked=True, timeout=1000
        ),
    )

    await ops_test.model.integrate(f"{APP_NAME}:fs-share", "server1:fs-share")
    await ops_test.model.integrate(f"{APP_NAME}:fs-share", "server2:fs-share")

    await ops_test.model.wait_for_idle(apps=[APP_NAME, "server1", "server2"], status="active", timeout=1000)
