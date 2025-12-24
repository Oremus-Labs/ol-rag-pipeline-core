from __future__ import annotations

import pytest

from ol_rag_pipeline_core.vpn import VpnRotationGuard, is_probably_external_url


class FakeGluetun:
    def __init__(self, *, status: str = "running"):
        self.status = status
        self.set_calls: list[str] = []

    def openvpn_status(self) -> str:
        return self.status

    def set_openvpn_status(self, status: str) -> None:
        self.set_calls.append(status)
        self.status = status


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://www.vatican.va/foo", True),
        ("http://example.com", True),
        ("http://prefect-server.prefect.svc.cluster.local:4200/api", False),
        ("http://qdrant.research.svc.cluster.local:6333", False),
        ("http://127.0.0.1:8000/v1/openvpn/status", False),
        ("file:///tmp/x", False),
        ("", False),
    ],
)
def test_is_probably_external_url(url: str, expected: bool) -> None:
    assert is_probably_external_url(url) is expected


def test_vpn_guard_rotates_every_n_requests() -> None:
    gluetun = FakeGluetun(status="running")
    guard = VpnRotationGuard(gluetun=gluetun, rotate_every_n_requests=3, require_vpn_for_external=True)

    assert guard.before_request("https://example.com/1") is False
    assert guard.before_request("https://example.com/2") is False
    assert guard.before_request("https://example.com/3") is True

    # rotate = stopped then running
    assert gluetun.set_calls == ["stopped", "running"]

