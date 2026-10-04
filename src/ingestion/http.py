"""HTTP с повторами и сохранением исходного ответа и происхождения данных."""
import hashlib
import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from .sources import now


def atomic_write(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_bytes(content)
    temp.replace(path)


class HttpClient:
    def __init__(self, root, timeout=30, attempts=3, offline=False, refresh=False):
        self.root = Path(root)
        self.timeout, self.attempts = timeout, attempts
        self.offline, self.refresh = offline, refresh
        self.requests = []

    def get(self, source, url, max_age=None):
        digest = hashlib.sha256(url.encode()).hexdigest()[:24]
        path = self.root / source / f"{digest}.response"
        meta_path = path.with_suffix(".meta.json")
        meta = None
        if path.exists() and meta_path.exists():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            content = path.read_bytes()
            if hashlib.sha256(content).hexdigest() != meta["sha256"] or meta["url"] != url:
                raise ValueError(f"Повреждён кэш: {path}")
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(meta["fetched_at"])).total_seconds()
            if self.offline or (not self.refresh and (max_age is None or age < max_age)):
                self.requests.append(dict(url=url, origin="cache", path=meta.get("raw_snapshot_path", str(path)), fetched_at=meta["fetched_at"]))
                return content, meta["fetched_at"]
        if self.offline:
            raise RuntimeError(f"Нет сохранённого ответа для офлайн-показа: {url}. Сначала выполните сетевую загрузку.")
        if meta and "raw_snapshot_path" not in meta:
            # Миграция раннего кэша: сохранить прошлый ответ до его обновления.
            version = meta["fetched_at"].replace(":", "").replace("+", "_")
            previous = path.parent / "snapshots" / digest / f"{version}.response"
            atomic_write(previous, content)
            atomic_write(previous.with_suffix(".meta.json"), json.dumps(meta, ensure_ascii=False, indent=2).encode())
        last = None
        for attempt in range(1, self.attempts + 1):
            try:
                request = urllib.request.Request(url, headers={"User-Agent": "marketplace-demand-forecast/0.1 (educational)"})
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    content = response.read()
                    meta = dict(url=url, status=response.status, fetched_at=now(),
                                content_type=response.headers.get("Content-Type"),
                                sha256=hashlib.sha256(content).hexdigest(), bytes=len(content))
                # Каждый сетевой ответ сохраняется неизменно, в том числе все
                # версии прогнозов внутри одного дня; URL-кэш хранит последний.
                version = meta["fetched_at"].replace(":", "").replace("+", "_")
                snapshot = path.parent / "snapshots" / digest / f"{version}.response"
                meta["raw_snapshot_path"] = str(snapshot)
                atomic_write(snapshot, content)
                atomic_write(snapshot.with_suffix(".meta.json"), json.dumps(meta, ensure_ascii=False, indent=2).encode())
                atomic_write(path, content)
                atomic_write(meta_path, json.dumps(meta, ensure_ascii=False, indent=2).encode())
                self.requests.append(dict(**meta, origin="http", path=str(snapshot), attempt=attempt))
                return content, meta["fetched_at"]
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                last = error
                self.requests.append(dict(url=url, origin="error", attempt=attempt, error=str(error)))
                if isinstance(error, urllib.error.HTTPError) and error.code not in {429, 500, 502, 503, 504}:
                    break
                if attempt < self.attempts:
                    time.sleep(min(2 ** (attempt - 1), 4))
        raise RuntimeError(f"HTTP-загрузка не выполнена: {url}: {last}")
