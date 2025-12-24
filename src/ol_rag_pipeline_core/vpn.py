from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlparse

import httpx


class VpnError(RuntimeError):
    pass


def is_probably_external_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    if host in {"localhost", "127.0.0.1"}:
        return False
    if host.endswith(".svc") or host.endswith(".svc.cluster.local"):
        return False
    return True


class GluetunControl(Protocol):
    def openvpn_status(self) -> str: ...
    def set_openvpn_status(self, status: str) -> None: ...


@dataclass(frozen=True)
class GluetunConfig:
    control_url: str = "http://127.0.0.1:8000"
    api_key: str | None = None
    timeout_s: float = 2.0


class GluetunHttpControlClient:
    def __init__(self, cfg: GluetunConfig):
        self._cfg = cfg

    def _client(self) -> httpx.Client:
        headers: dict[str, str] = {}
        if self._cfg.api_key:
            headers["X-API-Key"] = self._cfg.api_key
        return httpx.Client(
            base_url=self._cfg.control_url,
            timeout=self._cfg.timeout_s,
            headers=headers,
            follow_redirects=True,
            trust_env=False,
        )

    def openvpn_status(self) -> str:
        with self._client() as client:
            r = client.get("/v1/openvpn/status")
            r.raise_for_status()
            data = r.json()
        status = str(data.get("status") or "").lower()
        if status not in {"running", "stopped"}:
            raise VpnError(f"Unexpected Gluetun status payload: {data}")
        return status

    def set_openvpn_status(self, status: str) -> None:
        desired = status.lower()
        if desired not in {"running", "stopped"}:
            raise ValueError("status must be 'running' or 'stopped'")
        with self._client() as client:
            r = client.put("/v1/openvpn/status", json={"status": desired})
            r.raise_for_status()


@dataclass
class VpnRotationGuard:
    gluetun: GluetunControl
    rotate_every_n_requests: int = 10
    require_vpn_for_external: bool = True
    ensure_timeout_s: float = 90.0
    ensure_poll_s: float = 2.0
    rotate_cooldown_s: float = 2.0

    _external_request_count: int = 0

    def ensure_vpn_running(self) -> None:
        deadline = time.monotonic() + self.ensure_timeout_s
        last_status: str | None = None
        while time.monotonic() < deadline:
            try:
                last_status = self.gluetun.openvpn_status()
            except Exception as e:  # noqa: BLE001
                last_status = None
                time.sleep(self.ensure_poll_s)
                continue

            if last_status == "running":
                return
            try:
                self.gluetun.set_openvpn_status("running")
            except Exception:
                pass
            time.sleep(self.ensure_poll_s)

        raise VpnError(f"VPN not running after {self.ensure_timeout_s}s (last_status={last_status!r})")

    def rotate_vpn(self) -> None:
        # Force a new server selection by cycling OpenVPN.
        self.gluetun.set_openvpn_status("stopped")
        # Some providers keep status="running" briefly; just wait until it isn't running anymore.
        deadline = time.monotonic() + self.ensure_timeout_s
        while time.monotonic() < deadline:
            if self.gluetun.openvpn_status() != "running":
                break
            time.sleep(self.ensure_poll_s)

        self.gluetun.set_openvpn_status("running")
        self.ensure_vpn_running()
        time.sleep(self.rotate_cooldown_s)

    def before_request(self, url: str) -> bool:
        if not is_probably_external_url(url):
            return False

        if self.require_vpn_for_external:
            self.ensure_vpn_running()

        self._external_request_count += 1
        if self.rotate_every_n_requests > 0 and self._external_request_count % self.rotate_every_n_requests == 0:
            self.rotate_vpn()
            return True
        return False

