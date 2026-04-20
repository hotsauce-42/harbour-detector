from datetime import datetime
from typing import Optional
from pydantic import BaseModel, field_validator


class StopEvent(BaseModel):
    mmsi: int
    lat: float
    lon: float
    timestamp_start: datetime
    timestamp_end: datetime
    duration_minutes: float
    n_messages: int
    pos_variance_meters: float
    nav_status: Optional[int] = None
    draught_arrival: Optional[float] = None
    draught_departure: Optional[float] = None
    draught_delta: Optional[float] = None
    detection_method: str  # 'nav_status', 'sustained_speed', 'both'

    @field_validator("mmsi")
    @classmethod
    def mmsi_must_be_valid(cls, v: int) -> int:
        if not (100_000_000 <= v <= 999_999_999):
            raise ValueError(f"invalid MMSI: {v}")
        return v

    @field_validator("lat")
    @classmethod
    def lat_in_range(cls, v: float) -> float:
        if not (-90 <= v <= 90):
            raise ValueError(f"lat out of range: {v}")
        return v

    @field_validator("lon")
    @classmethod
    def lon_in_range(cls, v: float) -> float:
        if not (-180 <= v <= 180):
            raise ValueError(f"lon out of range: {v}")
        return v
