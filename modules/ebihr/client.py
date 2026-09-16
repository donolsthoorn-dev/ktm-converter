"""eBihr API client: V3 catalogs + VSE exports (rate-limited)."""

from __future__ import annotations

import logging
import os
import shutil
import time
from collections import deque
from pathlib import Path
from zipfile import ZipFile

try:
    import requests
except ImportError as e:  # pragma: no cover
    raise SystemExit("Installeer requests: pip install requests") from e

from modules.env_loader import load_dotenv
from modules.ebihr.constants import ROOT

log = logging.getLogger("ebihr.client")

API_BASE_DEFAULT = "https://api.bihr.net/api"
VSE_BASE = "https://vse.bihr.net/api/v1"
V3_CATALOGS = ("Products", "Prices", "Stocks", "SalesCategories")
VSE_EXPORTS = ("links", "vehicles")

_TIMEOUT = (15, 600)
_api_call_times: deque[float] = deque(maxlen=5)


def load_bihr_env() -> tuple[str, str, str]:
    """Return (api_base, username, password) from .env.bihr."""
    load_dotenv(ROOT / ".env.bihr", override=True)
    user = (os.environ.get("BIHR_USERNAME") or "").strip()
    password = (os.environ.get("BIHR_PASSWORD") or "").strip()
    base = (os.environ.get("BIHR_API_BASE") or API_BASE_DEFAULT).strip().rstrip("/")
    if not user or not password:
        raise SystemExit(
            "Bihr-credentials ontbreken (BIHR_USERNAME + BIHR_PASSWORD in .env.bihr)."
        )
    return base, user, password


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def rate_limit(url: str, *, heavy: bool = True) -> None:
    """
    Max 5 calls/min and min 10s between *heavy* calls (parity oude api.py).

    Status-polls (heavy=False) only enforce a short gap so long-running
    catalog builds are not delayed ~10s per poll.
    """
    now = time.time()
    min_gap = 10.0 if heavy else 1.2
    if _api_call_times:
        since_last = now - _api_call_times[-1]
        if since_last < min_gap:
            sleep_time = min_gap - since_last + 0.05
            log.info("Rate limit: wait %.1fs before %s", sleep_time, url)
            time.sleep(sleep_time)
            now = time.time()
    if heavy and len(_api_call_times) == 5:
        since_oldest = now - _api_call_times[0]
        if since_oldest < 60:
            sleep_time = 60 - since_oldest + 0.5
            log.info("Rate limit (5/min): wait %.1fs before %s", sleep_time, url)
            time.sleep(sleep_time)
            now = time.time()
    _api_call_times.append(now)
    log.info("API call %s: %s", time.strftime("%H:%M:%S"), url)


class BihrClient:
    def __init__(self, api_base: str | None = None, username: str | None = None, password: str | None = None):
        base, user, pw = load_bihr_env()
        self.api_base = (api_base or base).rstrip("/")
        self.username = username or user
        self.password = password or pw
        self.sess = _session()
        self.token: str | None = None

    def authenticate(self) -> str:
        url = f"{self.api_base}/v3/Authentication/Token"
        rate_limit(url)
        r = self.sess.post(
            url,
            data={"UserName": self.username, "PassWord": self.password},
            timeout=_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        r.raise_for_status()
        self.token = r.json()["access_token"]
        log.info("Bihr V3 token verkregen")
        return self.token

    def _headers(self) -> dict[str, str]:
        if not self.token:
            self.authenticate()
        assert self.token
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }

    def request_catalog(
        self,
        catalog_type: str,
        *,
        language: str = "nl",
        serialization: str = "JSON",
        compression: str = "ZIP",
    ) -> str:
        """POST Catalogs/Request → requestId."""
        if catalog_type not in V3_CATALOGS:
            raise ValueError(f"Onbekende catalogType {catalog_type!r}")
        url = f"{self.api_base}/v3/Catalogs/Request"
        body = {
            "catalogType": catalog_type,
            "compressionType": compression,
            "language": language,
            "serializationType": serialization,
        }
        rate_limit(url)
        r = self.sess.post(
            url,
            headers={**self._headers(), "Content-Type": "application/json"},
            json=body,
            timeout=_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        r.raise_for_status()
        data = r.json()
        req_id = data.get("id") or data.get("Id")
        if not req_id:
            raise RuntimeError(f"Geen request id in antwoord: {data!r}")
        log.info("Catalog request %s → id=%s", catalog_type, req_id)
        return str(req_id)

    def wait_catalog(self, request_id: str, *, poll_s: float = 3.0) -> str:
        """Poll tot Finished; return catalogFileId."""
        url = f"{self.api_base}/v3/Catalogs/Request/{request_id}"
        while True:
            rate_limit(url, heavy=False)
            r = self.sess.get(
                url,
                headers=self._headers(),
                timeout=_TIMEOUT,
                proxies={"http": None, "https": None},
            )
            r.raise_for_status()
            data = r.json()
            status = (data.get("status") or data.get("Status") or "").strip()
            file_id = data.get("catalogFileId") or data.get("CatalogFileId")
            err = data.get("errorMessage") or data.get("ErrorMessage")
            log.info("  Catalog %s status=%s fileId=%s", request_id[:8], status, file_id)
            if status == "Finished":
                if not file_id:
                    raise RuntimeError(f"Finished zonder catalogFileId: {data!r}")
                return str(file_id)
            if status in ("Failed", "Cancelled"):
                raise RuntimeError(f"Catalog request {status}: {err or data!r}")
            time.sleep(poll_s)

    def download_catalog_file(self, file_id: str, dest_zip: Path) -> Path:
        url = f"{self.api_base}/v3/Catalogs/File/{file_id}"
        rate_limit(url)
        dest_zip.parent.mkdir(parents=True, exist_ok=True)
        with self.sess.get(
            url,
            headers=self._headers(),
            stream=True,
            timeout=_TIMEOUT,
            proxies={"http": None, "https": None},
        ) as r:
            r.raise_for_status()
            with dest_zip.open("wb") as f:
                for chunk in r.iter_content(1024 * 256):
                    if chunk:
                        f.write(chunk)
        log.info("Catalog file opgeslagen: %s (%s bytes)", dest_zip, dest_zip.stat().st_size)
        return dest_zip

    def fetch_catalog(
        self,
        catalog_type: str,
        out_dir: Path,
        *,
        language: str = "nl",
        serialization: str = "JSON",
        compression: str = "ZIP",
    ) -> Path:
        """Request + poll + download + extract → returns extract dir for this catalog."""
        out_dir.mkdir(parents=True, exist_ok=True)
        req_id = self.request_catalog(
            catalog_type,
            language=language,
            serialization=serialization,
            compression=compression,
        )
        file_id = self.wait_catalog(req_id)
        zip_path = out_dir / f"v3_{catalog_type}.zip"
        self.download_catalog_file(file_id, zip_path)
        extract_dir = out_dir / f"v3_{catalog_type}"
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir(parents=True)
        with ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
        log.info("Uitgepakt → %s (%d files)", extract_dir, sum(1 for _ in extract_dir.rglob("*")))
        return extract_dir

    def fetch_vse(self, export: str, out_dir: Path) -> Path:
        """GET VSE Export/{links|vehicles}, extract into out_dir/vse_{export}/."""
        if export not in VSE_EXPORTS:
            raise ValueError(f"Onbekende VSE export {export!r}")
        if not self.token:
            self.authenticate()
        url = f"{VSE_BASE}/Export/{export}"
        rate_limit(url)
        out_dir.mkdir(parents=True, exist_ok=True)
        zip_path = out_dir / f"vse_{export}.zip"
        with self.sess.get(
            url,
            headers=self._headers(),
            stream=True,
            timeout=_TIMEOUT,
            proxies={"http": None, "https": None},
        ) as r:
            r.raise_for_status()
            with zip_path.open("wb") as f:
                for chunk in r.iter_content(1024 * 256):
                    if chunk:
                        f.write(chunk)
        extract_dir = out_dir / f"vse_{export}"
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir(parents=True)
        with ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
        log.info("VSE %s → %s", export, extract_dir)
        return extract_dir

    def fetch_all(
        self,
        out_dir: Path,
        *,
        catalogs: tuple[str, ...] = ("Products", "Prices", "Stocks"),
        language: str = "nl",
        include_vse: bool = True,
    ) -> Path:
        """Fetch V3 catalogs (+ VSE) into out_dir. Returns out_dir."""
        out_dir.mkdir(parents=True, exist_ok=True)
        self.authenticate()
        for cat in catalogs:
            log.info("=== V3 catalog %s ===", cat)
            self.fetch_catalog(cat, out_dir, language=language)
        if include_vse:
            log.info("=== VSE vehicles ===")
            self.fetch_vse("vehicles", out_dir)
            log.info("=== VSE links ===")
            self.fetch_vse("links", out_dir)
        (out_dir / "FETCH_OK").write_text(
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + "\n",
            encoding="utf-8",
        )
        return out_dir
