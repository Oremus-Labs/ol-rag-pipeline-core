from __future__ import annotations

import asyncio

from nats.aio.client import Client as NATS


async def publish_json(nats_url: str, subject: str, payload_json: str) -> None:
    nc = NATS()
    await nc.connect(servers=[nats_url])
    await nc.publish(subject, payload_json.encode("utf-8"))
    await nc.flush(timeout=2)
    await nc.close()


def publish_json_sync(nats_url: str, subject: str, payload_json: str) -> None:
    asyncio.run(publish_json(nats_url, subject, payload_json))

