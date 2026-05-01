"""SKU alias registry for disambiguating variants in family datasheets."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_ALIAS_FILE = _REPO_ROOT / "src" / "stages" / "sku_aliases" / "aliases.json"


@dataclass(frozen=True, slots=True)
class AliasEntry:
    """Alias data for a single SKU."""

    aliases: list[str]
    disambiguation: str


class AliasRegistry:
    """In-memory lookup for SKU aliases."""

    def __init__(self, path: Path = _ALIAS_FILE) -> None:
        self._path = path
        self._data: dict[str, AliasEntry] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            logger.debug("No alias file found at %s; registry empty.", self._path)
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load alias file %s: %s", self._path, exc)
            return
        for key, entry in raw.items():
            if not isinstance(entry, dict):
                continue
            aliases = entry.get("aliases", [])
            disambiguation = entry.get("disambiguation", "")
            if aliases and disambiguation:
                self._data[key] = AliasEntry(
                    aliases=list(aliases),
                    disambiguation=str(disambiguation),
                )

    def lookup(self, manufacturer: str, model: str) -> Optional[AliasEntry]:
        """Return alias entry for the given manufacturer:model key."""
        return self._data.get(f"{manufacturer}:{model}")


# Module-level singleton for runtime reuse
_registry: Optional[AliasRegistry] = None


def get_registry() -> AliasRegistry:
    """Return the shared alias registry instance."""
    global _registry
    if _registry is None:
        _registry = AliasRegistry()
    return _registry
