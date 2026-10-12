from datetime import datetime
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, Field, model_validator

HEATING_SETPOINT_MIN_C = 15.0
HEATING_SETPOINT_MAX_C = 24.0
COOLING_SETPOINT_MIN_C = 20.0
COOLING_SETPOINT_MAX_C = 30.0
MIN_DUAL_SETPOINT_DEADBAND_C = 2.0


# `SchoolMetadata` defines the metadata for a specific school in the room catalog.
class SchoolMetadata(BaseModel):
    id: str
    label: str
    address: str | None = None
    lat: float | None = None
    lng: float | None = None
    school_level: str | None = None
    construction_year: int | None = None


# `RoomSimulationSupports` is derived at load time from catalog defaults and thermostat_type.
class RoomSimulationSupports(BaseModel):
    occupancy: bool
    heating_setpoint: bool
    cooling_setpoint: bool
    lighting_w_per_m2: bool
    infiltration_ach: bool

# `RoomSimulationConfig` defines the simulation default configuration and input fields.
class RoomSimulationConfig(BaseModel):
    occupancy: int = Field(ge=0)
    heating_setpoint: float = Field(
        ge=HEATING_SETPOINT_MIN_C,
        le=HEATING_SETPOINT_MAX_C,
    )
    cooling_setpoint: float | None = Field(
        default=None,
        ge=COOLING_SETPOINT_MIN_C,
        le=COOLING_SETPOINT_MAX_C,
    )
    lighting_w_per_m2: float = Field(default=10.0, ge=0)
    infiltration_ach: float | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_setpoint_order(self):
        if (
            self.cooling_setpoint is not None
            and self.cooling_setpoint - self.heating_setpoint
            < MIN_DUAL_SETPOINT_DEADBAND_C
        ):
            raise ValueError(
                "cooling_setpoint must be at least "
                f"{MIN_DUAL_SETPOINT_DEADBAND_C:g}°C above heating_setpoint"
            )
        return self

# `RoomSimulationDefaults` defines the default simulation configurations for a specific school room.
class RoomSimulationDefaults(RoomSimulationConfig):
    pass


# Schema for RoomStaticSchedules, which defines the static schedule names for a room in the catalog.
class RoomStaticSchedules(BaseModel):
    occupancy: str | None = None
    lighting: str | None = None
    equipment: str | None = None
    heating_availability: str | None = None
    hvac_availability: str | None = None
    thermostat_control: str | None = None
    secondary_thermostat_control: str | None = None
    ventilation: str | None = None
    infiltration: str | None = None
    activity: str | None = None
    heating_setpoint: str | None = None
    cooling_setpoint: str | None = None
    outdoor_co2: str | None = None

# Schema for RoomCatalogEntry, which defines the metadata and configuration for a room in the catalog.
class RoomCatalogEntry(BaseModel):
    id: str
    label: str
    physical_instance_count: int = Field(
        default=1,
        ge=1,
        le=500,
        description=(
            "Identical physical rooms represented by this single IDF; simulation scales energy only."
        ),
    )
    idf_file: str
    simulation_enabled: bool = True
    simulation_unavailable_reason: str | None = None
    zone_name: str
    people_object_name: str
    occupancy_schedule_name: str
    thermostat_type: Literal["single_heating", "dual_setpoint"]
    heating_schedule_name: str
    cooling_schedule_name: str | None = None
    defaults: RoomSimulationDefaults
    static_schedules: RoomStaticSchedules

    @model_validator(mode="after")
    def validate_simulation_catalog_configuration(self):
        expects_cooling = self.thermostat_type == "dual_setpoint"

        if not self.simulation_enabled and not self.simulation_unavailable_reason:
            raise ValueError(
                "simulation_unavailable_reason is required when simulation_enabled is false"
            )
        if self.simulation_enabled and self.simulation_unavailable_reason is not None:
            raise ValueError(
                "simulation_unavailable_reason must be null when simulation_enabled is true"
            )
        if expects_cooling and self.cooling_schedule_name is None:
            raise ValueError(
                "cooling_schedule_name is required when cooling_setpoint is supported"
            )
        if not expects_cooling and self.cooling_schedule_name is not None:
            raise ValueError(
                "cooling_schedule_name must be null when cooling_setpoint is unsupported"
            )
        if not expects_cooling and self.defaults.cooling_setpoint is not None:
            raise ValueError(
                "defaults.cooling_setpoint must be null when cooling_setpoint is unsupported"
            )
        if (
            self.thermostat_type == "dual_setpoint"
            and self.defaults.cooling_setpoint is not None
            and self.defaults.heating_setpoint >= self.defaults.cooling_setpoint
        ):
            raise ValueError(
                "defaults.heating_setpoint must be lower than defaults.cooling_setpoint for dual_setpoint rooms"
            )
        if self.defaults.infiltration_ach is not None and self.defaults.infiltration_ach < 0:
            raise ValueError("defaults.infiltration_ach must be non-negative when set")
        return self

# Schema for RoomMetadata, which extends RoomCatalogEntry with additional metadata fields for API responses.
class RoomMetadata(RoomCatalogEntry):
    supports: RoomSimulationSupports
    school_id: str
    idf_path: str
    idf_exists: bool
    simulatable: bool
    simulation_status: Literal["available", "disabled", "missing_idf"]


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


class SchoolEnvironmentDeviceCatalogEntry(BaseModel):
    school_id: str
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


class SchoolEnergyDeviceCatalogEntry(BaseModel):
    school_id: str
    devices: list[EnergyDeviceMetadata]


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
