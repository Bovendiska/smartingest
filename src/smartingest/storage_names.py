"""Pure naming rules for batches and tables (no database imports, so they are unit-testable)."""

from __future__ import annotations

import hashlib
import uuid

from smartingest.parsing import MAX_COLUMN_NAME_LENGTH, slugify_column

BATCH_NAMESPACE = uuid.UUID("6f1d3c8e-7a52-4c1b-9f0e-2b7d4a6e9c11")
DEMO_TABLE_PREFIX = "demo_"


def batch_id_for(owner: uuid.UUID | None, source: str, content: bytes) -> tuple[uuid.UUID, str]:
    """Same owner + source + file content = same batch id, so re-uploads are no-ops."""
    sha = hashlib.sha256(content).hexdigest()
    return uuid.uuid5(BATCH_NAMESPACE, f"{owner or 'admin'}:{source}:{sha}"), sha


def silver_table_for(owner: uuid.UUID | None, source: str) -> str:
    """Admin data goes to silver.<source>; each demo visitor gets isolated tables."""
    slug = slugify_column(source)
    if owner is None:
        return slug
    return f"{DEMO_TABLE_PREFIX}{owner.hex[:12]}_{slug}"[:MAX_COLUMN_NAME_LENGTH]
