from datetime import datetime
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, Field


# `SchoolMetadata` defines the metadata for a school in the catalog.
class SchoolMetadata(BaseModel):
    id: str
    label: str
    address: str | None = None
    lat: float | None = None
    lng: float | None = None
    school_level: str | None = None
    construction_year: int | None = None


class OverviewReading(BaseModel):
    value: float | int
    unit: str | None = None


class DeviceLatestOverviewResponse(BaseModel):
    device_id: str
    latest_event_time: AwareDatetime
    readings: dict[str, OverviewReading]


class DeviceHistoryBucketItem(BaseModel):
    device_id: str
    event_time: AwareDatetime
    measurements: dict[str, OverviewReading]


class DeviceHistoryResponse(BaseModel):
    device_id: str
    count: int
    items: list[DeviceHistoryBucketItem]


IAQPolicyMetric = Literal["co2", "pm25"]
IAQPeriodId = Literal["1m", "1h", "24h", "7d", "14d"]


class IAQMetricPolicy(BaseModel):
    label: str
    unit: str
    thresholds: dict[IAQPeriodId, float]


class IAQPolicyResponse(BaseModel):
    version: str
    metrics: dict[IAQPolicyMetric, IAQMetricPolicy]


class SchoolDeviceMetadata(BaseModel):
    id: str
    label: str
    room_id: str
    manufacturer: str = "University of Patras"
    model: str = "v1.0"


class EnvironmentDeviceAssignments(BaseModel):
    devices: list[SchoolDeviceMetadata]


EnergyLoadType = Literal["hvac", "lighting", "mixed", "other"]


class EnergyPhaseMetadata(BaseModel):
    load_type: EnergyLoadType
    target_room_id: str
    display_label: str | None = None


class EnergyThreePhaseChannels(BaseModel):
    a: EnergyPhaseMetadata
    b: EnergyPhaseMetadata
    c: EnergyPhaseMetadata


class EnergyPlugDeviceMetadata(BaseModel):
    type: Literal["plug"]
    id: str
    model: str
    manufacturer: str = "Shelly"
    room_id: str
    room_alias: str | None = None
    label: str
    load_type: EnergyLoadType


class EnergyThreePhaseDeviceMetadata(BaseModel):
    type: Literal["three_phase_meter"]
    id: str
    model: str
    manufacturer: str = "Shelly"
    room_id: str
    room_alias: str | None = None
    phases: EnergyThreePhaseChannels


EnergyDeviceMetadata = Annotated[
    EnergyPlugDeviceMetadata | EnergyThreePhaseDeviceMetadata,
    Field(discriminator="type"),
]


class EnergyDeviceAssignments(BaseModel):
    assume_school_total: bool = False
    devices: list[EnergyDeviceMetadata]


class SchoolDeviceCatalogEntry(BaseModel):
    school_id: str
    environment: EnvironmentDeviceAssignments
    energy: EnergyDeviceAssignments


# ============ Schemas for `/thermal-comfort` endpoints ============

# Schema for DiscomfortIndexResponse, which defines the response model for the Discomfort Index (DI) endpoint.
class DiscomfortIndexResponse(BaseModel):
    di: float
    discomfort_condition: str


# Schema for HumidexResponse, which defines the response model for the Humidex endpoint.
class HumidexResponse(BaseModel):
    humidex: float
    discomfort: str


# ============ School-level deterministic environmental insights ============

EnvironmentalInsightHorizon = Literal["short_term", "long_term"]
EnvironmentalInsightKind = Literal["co2", "pm25", "thermal", "overview"]
EnvironmentalInsightSeverity = Literal[
    "critical", "high", "medium", "low", "positive"
]


class EnvironmentalInsightEvidence(BaseModel):
    label: str
    value: str


class EnvironmentalInsightCard(BaseModel):
    id: str
    horizon: EnvironmentalInsightHorizon
    kind: EnvironmentalInsightKind
    severity: EnvironmentalInsightSeverity
    title: str
    summary: str
    period_label: str
    room_labels: list[str] = Field(default_factory=list)
    affected_rooms: int = 0
    index_label: Literal["Criticality index", "Stability index"]
    index_score: int = Field(ge=0, le=100)
    evidence: list[EnvironmentalInsightEvidence] = Field(default_factory=list)
    recommendation: str


class SchoolEnvironmentalInsightsResponse(BaseModel):
    school_id: str
    generated_at: datetime
    data_through: datetime | None = None
    total_rooms: int = 0
    rooms_analyzed: int = 0
    total_devices: int = 0
    devices_analyzed: int = 0
    policy_version: str
    insights: list[EnvironmentalInsightCard] = Field(default_factory=list)
    partial: bool = False
    warnings: list[str] = Field(default_factory=list)
    computation_ms: float = 0.0


# ============ School-level deterministic energy insights ============

EnergyInsightHorizon = Literal["short_term", "long_term"]
EnergyInsightKind = Literal[
    "after_hours",
    "baseload",
    "carbon_impact",
    "dominant_load",
    "persistent_baseload",
    "recent_increase",
    "recurring_peak",
    "stability",
]
EnergyInsightSeverity = Literal["critical", "high", "medium", "low", "positive"]
EnergyMeteringScope = Literal["whole_building", "metered_loads", "unavailable"]


class EnergyInsightEvidence(BaseModel):
    label: str
    value: str


class EnergyInsightCard(BaseModel):
    id: str
    horizon: EnergyInsightHorizon
    kind: EnergyInsightKind
    severity: EnergyInsightSeverity
    title: str
    summary: str
    period_label: str
    scope_labels: list[str] = Field(default_factory=list)
    index_label: Literal["Criticality index", "Stability index"]
    index_score: int = Field(ge=0, le=100)
    evidence: list[EnergyInsightEvidence] = Field(default_factory=list)
    recommendation: str


class EnergyHourlyBaselineBucket(BaseModel):
    weekday: int = Field(ge=0, le=6)
    hour: int = Field(ge=0, le=23)
    baseline_wh: float = Field(ge=0.0)


class SchoolEnergyInsightsResponse(BaseModel):
    school_id: str
    generated_at: datetime
    data_through: datetime | None = None
    policy_version: str
    emissions_factor_kg_per_kwh: float | None = Field(default=None, ge=0.0)
    emissions_factor_source: str | None = None
    emissions_factor_reference_year: int | None = None
    emissions_factor_version: str | None = None
    metering_scope: EnergyMeteringScope
    total_devices: int = 0
    devices_analyzed: int = 0
    coverage_pct: float = Field(default=0.0, ge=0.0, le=100.0)
    hourly_baseline_profile: list[EnergyHourlyBaselineBucket] = Field(
        default_factory=list
    )
    insights: list[EnergyInsightCard] = Field(default_factory=list)
    partial: bool = False
    warnings: list[str] = Field(default_factory=list)
    computation_ms: float = 0.0


class HomeEnvironmentRequest(BaseModel):
    stream: bool = False
    room_ids: list[str] = Field(min_length=1, max_length=8)
    refresh: bool = False


class HomeEnvironmentRoom(BaseModel):
    part: Literal["live", "history", "complete"] = "complete"
    room_id: str
    device_id: str
    live: DeviceHistoryResponse | None = None
    history: DeviceHistoryResponse | None = None
    live_fetched_at: datetime | None = None
    history_fetched_at: datetime | None = None
    errors: list[Literal['live_unavailable', 'history_unavailable', 'live_stale', 'history_stale']]


class HomeEnvironmentResponse(BaseModel):
    school_id: str
    rooms: list[HomeEnvironmentRoom]
