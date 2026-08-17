"""Persistent dynamic filtering state."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .adaptive_rules import AdaptiveState
from .context_rules import ContextState
from .event_rules import EventState
from .model_rules import ModelRuntimeState


STATE_SCHEMA_VERSION = 3


@dataclass
class DeduplicationEntry:
    """One deduplication ledger entry."""

    fingerprint: str
    device_id: str


@dataclass
class DynamicState:
    """Container for all dynamic state that can be persisted."""

    adaptive: AdaptiveState
    context: ContextState
    events: EventState
    deduplication: list[DeduplicationEntry]
    model: ModelRuntimeState

    @classmethod
    def empty(cls) -> "DynamicState":
        """Create an empty dynamic state."""

        return cls(
            adaptive=AdaptiveState(),
            context=ContextState(),
            events=EventState(),
            deduplication=[],
            model=ModelRuntimeState(),
        )

    def fingerprint_seen(self, fingerprint: str) -> bool:
        """Return whether a record fingerprint has already updated state."""

        return fingerprint in {entry.fingerprint for entry in self.deduplication}

    def mark_fingerprint(self, fingerprint: str, *, device_id: str, capacity: int) -> None:
        """Record a processed fingerprint with deterministic bounded eviction."""

        if self.fingerprint_seen(fingerprint):
            return
        self.deduplication.append(DeduplicationEntry(fingerprint=fingerprint, device_id=device_id))
        if len(self.deduplication) > capacity:
            del self.deduplication[: len(self.deduplication) - capacity]

    def reset_device(self, device_id: str) -> None:
        """Reset device-specific state where supported."""

        self.adaptive.reset_device(device_id)
        self.context.reset_device(device_id)
        self.events.reset_device(device_id)
        self.deduplication = [
            entry for entry in self.deduplication if entry.device_id != device_id
        ]

    def serialize(self) -> dict[str, Any]:
        """Serialize the full state to a JSON-compatible object."""

        return {
            "state_schema_version": STATE_SCHEMA_VERSION,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "adaptive": self.adaptive.serialize(),
            "context": self.context.serialize(),
            "events": self.events.serialize(),
            "deduplication": {
                "entries": [
                    {
                        "fingerprint": entry.fingerprint,
                        "device_id": entry.device_id,
                    }
                    for entry in self.deduplication
                ],
            },
            "model": self.model.serialize(),
        }

    @classmethod
    def restore(cls, payload: dict[str, Any]) -> "DynamicState":
        """Restore full dynamic state and migrate supported old versions."""

        if not isinstance(payload, dict):
            raise ValueError("dynamic state file must contain an object")
        version = payload.get("state_schema_version")
        if version == 1:
            return cls._restore_v1(payload)
        if version == 2:
            return cls._restore_v2(payload)
        if version != STATE_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported dynamic state version {version!r}; "
                f"expected {STATE_SCHEMA_VERSION}"
            )
        return cls._restore_current_payload(payload)

    @classmethod
    def _restore_v2(cls, payload: dict[str, Any]) -> "DynamicState":
        """Migrate v2 state by preserving dedup and decoding legacy-safe keys."""

        return cls._restore_current_payload(payload)

    @classmethod
    def _restore_v1(cls, payload: dict[str, Any]) -> "DynamicState":
        """Migrate v1 state by preserving usable state and clearing legacy dedup."""

        return cls(
            adaptive=AdaptiveState.restore(payload.get("adaptive", {})),
            context=ContextState.restore(payload.get("context", {})),
            events=EventState.restore(payload.get("events", {})),
            deduplication=[],
            model=ModelRuntimeState(),
        )

    @classmethod
    def _restore_current_payload(cls, payload: dict[str, Any]) -> "DynamicState":
        dedup = payload.get("deduplication", {})
        if not isinstance(dedup, dict):
            raise ValueError("dynamic state deduplication must be an object")
        entries = dedup.get("entries", [])
        if not isinstance(entries, list):
            raise ValueError("dynamic state deduplication.entries must be a list")
        dedup_entries: list[DeduplicationEntry] = []
        for item in entries:
            if not isinstance(item, dict):
                raise ValueError("dynamic state deduplication entries must be objects")
            fingerprint = item.get("fingerprint")
            device_id = item.get("device_id")
            if not isinstance(fingerprint, str) or not fingerprint.strip():
                raise ValueError("dynamic state deduplication entry fingerprint is invalid")
            if not isinstance(device_id, str) or not device_id.strip():
                raise ValueError("dynamic state deduplication entry device_id is invalid")
            dedup_entries.append(
                DeduplicationEntry(fingerprint=fingerprint.strip(), device_id=device_id.strip())
            )
        return cls(
            adaptive=AdaptiveState.restore(payload.get("adaptive", {})),
            context=ContextState.restore(payload.get("context", {})),
            events=EventState.restore(payload.get("events", {})),
            deduplication=dedup_entries,
            model=ModelRuntimeState.restore(payload.get("model")),
        )


def load_dynamic_state(path: str | Path | None) -> tuple[DynamicState, bool]:
    """Load dynamic state if a path exists, otherwise return an empty state."""

    if path is None:
        return DynamicState.empty(), False
    state_path = Path(path)
    if not state_path.exists():
        return DynamicState.empty(), False
    with state_path.open("r", encoding="utf-8") as handle:
        loaded = json.load(handle)
    return DynamicState.restore(loaded), True


def save_dynamic_state(state: DynamicState, path: str | Path) -> None:
    """Atomically save dynamic state to JSON."""

    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = state_path.with_name(f".{state_path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(state.serialize(), handle, indent=2, ensure_ascii=False, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp_path, state_path)
