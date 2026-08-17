"""Decode SMART Sleeper packet rows from MeshNET Influx exports."""

from __future__ import annotations

import struct
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import IntEnum
from typing import Any


class JsonIndex(IntEnum):
    TIMESTAMP = 0
    AREA = 1
    DEVICE_ID = 2
    EIGHTH_PACKET = 3
    ELEVENTH_PACKET = 4
    FIFTEENTH_PACKET = 5
    FIFTH_PACKET = 6
    FIRST_PACKET = 7
    FOURTEENTH_PACKET = 8
    FOURTH_PACKET = 9
    LATITUDE = 10
    LOCATION = 11
    LONGITUDE = 12
    NINTH_PACKET = 13
    SECOND_PACKET = 14
    SEVENTH_PACKET = 15
    SITE = 16
    SIXTH_PACKET = 17
    TENTH_PACKET = 18
    THIRD_PACKET = 19
    THIRTEENTH_PACKET = 20
    TWELFTH_PACKET = 21


PACKET_SIZE = 51
HEADER_SIZE = 4
PACKET_NUMBER_INDEX = 2
PACKET_FIELDS = (
    ("FirstPacket", JsonIndex.FIRST_PACKET),
    ("SecondPacket", JsonIndex.SECOND_PACKET),
    ("ThirdPacket", JsonIndex.THIRD_PACKET),
    ("FourthPacket", JsonIndex.FOURTH_PACKET),
    ("FifthPacket", JsonIndex.FIFTH_PACKET),
    ("SixthPacket", JsonIndex.SIXTH_PACKET),
    ("SeventhPacket", JsonIndex.SEVENTH_PACKET),
    ("EighthPacket", JsonIndex.EIGHTH_PACKET),
    ("NinthPacket", JsonIndex.NINTH_PACKET),
    ("TenthPacket", JsonIndex.TENTH_PACKET),
    ("EleventhPacket", JsonIndex.ELEVENTH_PACKET),
    ("TwelfthPacket", JsonIndex.TWELFTH_PACKET),
    ("ThirteenthPacket", JsonIndex.THIRTEENTH_PACKET),
    ("FourteenthPacket", JsonIndex.FOURTEENTH_PACKET),
    ("FifteenthPacket", JsonIndex.FIFTEENTH_PACKET),
)

# The environment values occupy the first 32 bytes of the 47-byte payload.
# Unsigned physical/time fields preserve firmware sentinels (0xffff/0xff),
# which the filtering layer already converts to missing values.
ENV_FRAME_STRUCT = struct.Struct("<hhhhhHBHHiiHBBBBB")


@dataclass(frozen=True)
class EnvFrame:
    rtd1_t: int
    rtd2_t: int
    rtd3_t: int
    rtd4_t: int
    tmp102_t: int
    moist_pc: int
    flood_flag: int
    rain_mm: int
    sleeper_rh: int
    lat: int
    lon: int
    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int

    @classmethod
    def decode(cls, data: bytes) -> "EnvFrame":
        if len(data) < ENV_FRAME_STRUCT.size:
            raise ValueError(
                f"Environment payload is {len(data)} bytes; expected at least {ENV_FRAME_STRUCT.size}"
            )
        return cls(*ENV_FRAME_STRUCT.unpack_from(data))


@dataclass(frozen=True)
class SMARTSleeperFrame:
    device_id: str
    timestamp: str
    area: str
    location: str
    latitude: float
    longitude: float
    env_frame: EnvFrame | None

    @classmethod
    def decode(cls, row: Mapping[str, Any] | Sequence[Any]) -> "SMARTSleeperFrame":
        """Decode one column-mapped row or the legacy 22-value row."""

        env_frame = None
        for field_name, index in PACKET_FIELDS:
            decoded = cls._decode_packet(_row_value(row, field_name, index))
            if decoded is not None:
                env_frame = decoded

        return cls(
            device_id=str(_row_value(row, "ControllerName", JsonIndex.DEVICE_ID)),
            timestamp=str(_row_value(row, "time", JsonIndex.TIMESTAMP)),
            area=str(_row_value(row, "Area", JsonIndex.AREA)),
            location=str(_row_value(row, "Location", JsonIndex.LOCATION)),
            latitude=float(_row_value(row, "Latitude", JsonIndex.LATITUDE)),
            longitude=float(_row_value(row, "Longitude", JsonIndex.LONGITUDE)),
            env_frame=env_frame,
        )

    @staticmethod
    def _decode_packet(value: Any) -> EnvFrame | None:
        if value is None or (isinstance(value, str) and value.strip().lower() == "null"):
            return None
        if not isinstance(value, str):
            raise ValueError(f"Packet must be a string or null, got {type(value).__name__}")

        raw_hex, separator, raw_port = value.partition(",")
        if not separator:
            raise ValueError("Packet must use '<hex>,<fport>' format")
        try:
            int(raw_port)
            packet = bytes.fromhex(raw_hex)
        except ValueError as exc:
            raise ValueError(f"Invalid packet encoding: {value!r}") from exc

        # Real exports contain short control/ack packets alongside sensor frames.
        if len(packet) != PACKET_SIZE or packet[PACKET_NUMBER_INDEX] != 0:
            return None

        payload = packet[HEADER_SIZE:]
        if packet[3] in {0x00, 0xFF} or all(byte == 0xFF for byte in payload):
            return None
        return EnvFrame.decode(payload)


def _row_value(
    row: Mapping[str, Any] | Sequence[Any],
    field_name: str,
    index: JsonIndex,
) -> Any:
    if isinstance(row, Mapping):
        if field_name not in row:
            raise ValueError(f"Missing expected column: {field_name}")
        return row[field_name]
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        try:
            return row[index]
        except IndexError as exc:
            raise ValueError(f"Row is missing index {int(index)} ({field_name})") from exc
    raise ValueError("SMART Sleeper row must be a mapping or sequence")
