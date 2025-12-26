from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol
import os
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
    def public_ip(self) -> str | None: ...


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

    def public_ip(self) -> str | None:
        with self._client() as client:
            r = client.get("/v1/publicip/ip")
            r.raise_for_status()
            data = r.json()
        if isinstance(data, str):
            ip = data.strip()
            return ip or None
        if isinstance(data, dict):
            ip = str(data.get("public_ip") or data.get("ip") or "").strip()
            return ip or None
        return None


@dataclass
class VpnRotationGuard:
    gluetun: GluetunControl | None = None
    # Optional proxy pool (comma-separated list via env in control plane).
    # When set, we rotate external requests by switching the pod's HTTP(S) proxy env vars,
    # rather than restarting OpenVPN (which would disrupt all workers sharing the same egress).
    proxy_pool: list[str] | None = None
    rotate_every_n_requests: int = 10
    require_vpn_for_external: bool = True
    ensure_timeout_s: float = 90.0
    ensure_poll_s: float = 2.0
    rotate_cooldown_s: float = 2.0
    # Cache successful health checks to avoid probing on every request.
    # This is important for proxy-pool mode: if we probe an external IP endpoint per request,
    # we double the number of outbound calls and slow the crawl dramatically.
    status_cache_ttl_s: float = 30.0

    _external_request_count: int = 0
    _proxy_index: int = 0
    _last_ok_monotonic: float = 0.0

    def _apply_proxy_env(self, proxy_url: str | None) -> None:
        if not proxy_url:
            return
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        os.environ["ALL_PROXY"] = proxy_url

    def _current_proxy(self) -> str | None:
        if self.proxy_pool:
            return os.environ.get("HTTP_PROXY") or None
        return None

    def _is_cache_fresh(self) -> bool:
        if self.status_cache_ttl_s <= 0:
            return False
        return (time.monotonic() - self._last_ok_monotonic) < self.status_cache_ttl_s

    def _rotate_proxy_once(self) -> str:
        assert self.proxy_pool
        self._proxy_index = (self._proxy_index + 1) % len(self.proxy_pool)
        proxy = self.proxy_pool[self._proxy_index]
        self._apply_proxy_env(proxy)
        return proxy

    def _probe_external_connectivity(self) -> bool:
        """
        Verify external connectivity through the pod's configured egress.

        We intentionally use an external probe URL (default: ipinfo.io) as a fallback because
        Gluetun's `/v1/publicip/ip` can be empty depending on DNS mode and internal settings.
        """
        probe_urls = [
            # Common plain-text IP endpoints.
            "https://ipinfo.io/ip",
            "https://api.ipify.org",
            "https://ifconfig.co/ip",
            "https://icanhazip.com",
            # Cloudflare trace: includes a line like `ip=1.2.3.4`
            "https://cloudflare.com/cdn-cgi/trace",
        ]
        with httpx.Client(timeout=5.0, follow_redirects=True) as client:
            for url in probe_urls:
                try:
                    r = client.get(url)
                    if r.status_code >= 400:
                        continue
                    text = (r.text or "").strip()
                    if not text:
                        continue
                    if "cdn-cgi/trace" in url:
                        for line in text.splitlines():
                            if line.startswith("ip="):
                                ip = line.split("=", 1)[1].strip()
                                return bool(ip) and any(ch.isdigit() for ch in ip)
                        continue
                    return any(ch.isdigit() for ch in text)
                except Exception:  # noqa: BLE001
                    continue
        return False

    def ensure_vpn_running(self) -> None:
        if self._is_cache_fresh():
            return

        deadline = time.monotonic() + self.ensure_timeout_s
        last_status: str | None = None
        last_ip: str | None = None
        while time.monotonic() < deadline:
            # Proxy-pool mode: we only verify that the current proxy can reach the outside world.
            # The proxy pods (Gluetun egress) manage their own OpenVPN state independently.
            if self.proxy_pool:
                if not self._current_proxy():
                    self._apply_proxy_env(self.proxy_pool[0])
                # Prefer checking the egress OpenVPN state via the control API if provided.
                # This avoids hitting an external IP endpoint for every request.
                if self.gluetun:
                    try:
                        last_status = self.gluetun.openvpn_status()
                    except Exception:
                        last_status = None
                    if last_status == "running":
                        self._last_ok_monotonic = time.monotonic()
                        return
                # Fallback: probe externally (rate-limited by cache TTL).
                if self._probe_external_connectivity():
                    self._last_ok_monotonic = time.monotonic()
                    return
                try:
                    self._rotate_proxy_once()
                except Exception:
                    pass
                time.sleep(self.ensure_poll_s)
                continue

            try:
                if not self.gluetun:
                    raise VpnError("VPN is required but no Gluetun control client is configured")
                last_status = self.gluetun.openvpn_status()
            except Exception as e:  # noqa: BLE001
                last_status = None
                time.sleep(self.ensure_poll_s)
                continue

            if last_status == "running":
                # If Gluetun reports the tunnel as running, treat it as "good enough" and
                # avoid expensive external probes on every request. Failures will be surfaced
                # by the caller's actual HTTP request and can trigger rotations/retries.
                try:
                    last_ip = self.gluetun.public_ip()
                except Exception:
                    last_ip = None
                if last_ip:
                    self._last_ok_monotonic = time.monotonic()
                    return
                if self._probe_external_connectivity():
                    self._last_ok_monotonic = time.monotonic()
                    return
                # Gluetun can report status="running" while the tunnel isn't fully usable;
                # force a reconnect in that case.
                try:
                    self.gluetun.set_openvpn_status("stopped")
                except Exception:
                    pass
                try:
                    self.gluetun.set_openvpn_status("running")
                except Exception:
                    pass
            else:
                try:
                    self.gluetun.set_openvpn_status("running")
                except Exception:
                    pass
            time.sleep(self.ensure_poll_s)

        raise VpnError(
            f"VPN not running after {self.ensure_timeout_s}s (last_status={last_status!r} public_ip={last_ip!r})"
        )

    def rotate_vpn(self) -> None:
        if self.proxy_pool:
            if not self.proxy_pool:
                raise VpnError("VPN proxy pool is empty")
            if len(self.proxy_pool) == 1:
                # Nothing to rotate to; just make sure it's healthy.
                self.ensure_vpn_running()
                time.sleep(self.rotate_cooldown_s)
                return
            # Try each proxy at most once during a rotation attempt.
            for _ in range(len(self.proxy_pool)):
                self._rotate_proxy_once()
                try:
                    self.ensure_vpn_running()
                    time.sleep(self.rotate_cooldown_s)
                    return
                except Exception:
                    continue
            raise VpnError("All VPN proxies failed connectivity checks")

        # Force a new server selection by cycling OpenVPN.
        if not self.gluetun:
            raise VpnError("rotate_vpn requires a Gluetun control client or a proxy pool")
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
        rotate_every = self.rotate_every_n_requests
        if self.proxy_pool and len(self.proxy_pool) <= 1:
            rotate_every = 0
        if rotate_every > 0 and self._external_request_count % rotate_every == 0:
            self.rotate_vpn()
            return True
        return False
