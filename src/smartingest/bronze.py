"""Bronze layer: raw uploaded files, stored on a local volume or any S3-compatible service."""

from __future__ import annotations

import io
import logging
import uuid
from datetime import date
from pathlib import Path

from smartingest.config import Settings

logger = logging.getLogger(__name__)


class BronzeStore:
    """Raw uploaded files, never modified. Keys are built only from values we generated."""

    def __init__(self, settings: Settings) -> None:
        self._backend = settings.bronze_backend
        self._bucket = settings.bronze_bucket
        self._root = Path(settings.bronze_local_path).resolve()
        if self._backend == "s3":
            from minio import Minio  # any S3-compatible service: AWS S3, Cloudflare R2, Backblaze B2, ...

            self._client = Minio(
                settings.s3_endpoint,
                access_key=settings.s3_access_key,
                secret_key=settings.s3_secret_key,
                secure=settings.s3_secure,
            )

    @staticmethod
    def make_key(table: str, batch_id: uuid.UUID, ext: str) -> str:
        # A random suffix per attempt: a failed concurrent upload can only ever delete its own object.
        return f"{table}/{date.today():%Y/%m/%d}/{batch_id}/original-{uuid.uuid4().hex[:8]}{ext}"

    def _local_path(self, key: str) -> Path:
        path = (self._root / key).resolve()
        if self._root not in path.parents:  # defence in depth against path traversal
            raise ValueError(f"unsafe bronze key: {key}")
        return path

    def put(self, table: str, batch_id: uuid.UUID, ext: str, content: bytes) -> str:
        key = self.make_key(table, batch_id, ext)
        if self._backend == "s3":
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket)
            self._client.put_object(self._bucket, key, io.BytesIO(content), length=len(content))
            return key
        path = self._local_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(content)
        tmp.replace(path)  # atomic: readers never see a half-written file
        return key

    def delete(self, keys: list[str]) -> None:
        if not keys:
            return
        if self._backend == "s3":
            from minio.deleteobjects import DeleteObject

            for error in self._client.remove_objects(self._bucket, [DeleteObject(k) for k in keys]):
                logger.error("Could not delete bronze object: %s", error)  # iterating performs the deletes
            return
        for key in keys:
            try:
                self._local_path(key).unlink(missing_ok=True)
            except (OSError, ValueError):
                logger.exception("Could not delete bronze file %s", key)
