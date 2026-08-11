"""Deletes every tenant under the CI key's tenant group.

Most of the live suite creates a tenant per test and never deletes it — a
non-issue against docker-compose's ephemeral database, but against a
persistent backend (e.g. the cloud nightly run) it accumulates forever. Run
this once at the end of a suite run to sweep it all up.

Usage: python tests/cleanup_cloud_tenants.py
Requires BOUNDFLOW_API_KEY and BOUNDFLOW_SERVER_ADDRESS.
"""
from __future__ import annotations

import asyncio
import os

from boundflow import ControlPlaneClient, FailedPreconditionError, NotFoundError


async def main() -> None:
    async with ControlPlaneClient(
        os.environ["BOUNDFLOW_SERVER_ADDRESS"], api_key=os.environ["BOUNDFLOW_API_KEY"]
    ) as cp:
        tenants = await cp.list_tenants()
        pending = [t.id for t in tenants if t.deleted_at is None]
        print(f"{len(pending)} tenant(s) to clean up")

        ok = 0
        deferred = 0
        for tenant_id in pending:
            try:
                await cp.delete_tenant(tenant_id)
                ok += 1
            except (FailedPreconditionError, NotFoundError):
                # Still draining workflows, or already gone by the time we got here —
                # both expected, not failures.
                deferred += 1

        print(f"done: {ok} deleted, {deferred} deferred/already-gone")


if __name__ == "__main__":
    asyncio.run(main())
