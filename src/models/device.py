"""Device specification models for PatchLang generation."""

from typing import Optional

from pydantic import BaseModel, Field


class TemplateMetadata(BaseModel):
    """Device metadata for template definition."""

    manufacturer: str
    model: str
    category: str  # console, stagebox, router, converter, card, rf-system, etc.
    kind: Optional[str] = None  # device, card, fixed-converter, stage-core, etc.
    description: Optional[str] = None
    dante_chipset: Optional[str] = None  # Ultimo, Brooklyn_II, Brooklyn_3, Broadway, HC
    rf_subtype: Optional[str] = None  # radio-mic, iem, bidirectional
    rf_min_channels: Optional[int] = None
    rf_max_channels: Optional[int] = None
    rf_band: Optional[str] = None


class PortDefinition(BaseModel):
    """A port or port range on a device."""

    name: str  # e.g., "Dante_In", "Mic_Input"
    direction: str  # "in", "out", or "io"
    start: int = 1
    end: Optional[int] = None  # None means single port, not an array
    connector: Optional[str] = None  # XLR, BNC_75, RJ45_etherCON, USB, etc.
    protocols: list[str] = Field(default_factory=list)  # Dante, MADI, AES3, Analogue, etc.
    attributes: list[str] = Field(default_factory=list)  # redundant, flexible, etc.

    @property
    def port_count(self) -> int:
        """Return the number of channels this port represents."""
        if self.end is None:
            return 1
        return self.end - self.start + 1

    def patchlang_name(self) -> str:
        """Return the PatchLang port name."""
        if self.end is None:
            return self.name
        return f"{self.name}[{self.start}..{self.end}]"

    def patchlang_definition(self) -> str:
        """Return the PatchLang port definition line."""
        parts = [self.patchlang_name(), ":", self.direction]
        if self.connector:
            parts.append(f"({self.connector})")
        if self.protocols or self.attributes:
            attrs = self.protocols + self.attributes
            parts.append(f"[{', '.join(attrs)}]")
        return " ".join(parts)


class BridgeRule(BaseModel):
    """Internal signal routing rule (bridge in PatchLang)."""

    source_port: str  # Port name (may include range)
    dest_port: str  # Port name (may include range)
    mapping: Optional[str] = None  # "1:1", "offset 16", etc.


class DeviceSpec(BaseModel):
    """Complete device specification extracted from manual."""

    manufacturer: str
    model: str
    metadata: TemplateMetadata
    ports: list[PortDefinition] = Field(default_factory=list)
    bridges: list[BridgeRule] = Field(default_factory=list)
    slots: Optional[dict[str, str]] = None  # {slot_name: format}
    internal_buses: Optional[list[str]] = None  # Named buses within the device
    streams: Optional[list[str]] = None  # Virtual channels (Dante/AES67)
    notes: str = ""

    def has_dante(self) -> bool:
        """Check if device has Dante ports."""
        return any("Dante" in p.protocols for p in self.ports)

    def has_madi(self) -> bool:
        """Check if device has MADI ports."""
        return any("MADI" in p.protocols for p in self.ports)

    def dante_channel_count(self) -> tuple[int, int]:
        """Return (in_channels, out_channels) for Dante."""
        in_count = sum(
            p.port_count for p in self.ports
            if "Dante" in p.protocols and p.direction == "in"
        )
        out_count = sum(
            p.port_count for p in self.ports
            if "Dante" in p.protocols and p.direction == "out"
        )
        return (in_count, out_count)


class DeviceInput(BaseModel):
    """Input device metadata before extraction."""

    id: str  # manufacturer-model (normalized lowercase-with-hyphens)
    manufacturer: str
    model: str
    category: str  # From EasySchematic or Patchify
    description: Optional[str] = None
