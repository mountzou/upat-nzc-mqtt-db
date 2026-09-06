from datetime import date, datetime, time, timedelta
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, model_validator

HEATING_SETPOINT_MIN_C = 15.0
HEATING_SETPOINT_MAX_C = 24.0
COOLING_SETPOINT_MIN_C = 20.0
COOLING_SETPOINT_MAX_C = 30.0
MIN_DUAL_SETPOINT_DEADBAND_C = 2.0

SimulationHorizon = Literal["forecast_1d", "forecast_7d", "tmy_30d", "tmy_182d", "custom"]

# `SchoolMetadata` defines the metadata for a specific school in the room catalog.
class SchoolMetadata(BaseModel):
    id: str
    label: str
    address: str | None = None
    lat: float | None = None
    lng: float | None = None
    school_level: str | None = None
    construction_year: int | None = None


class SimulationBaselineClosure(BaseModel):
    name: str
    start_date: date
    end_date: date
    source: Literal["idf_default"] = "idf_default"


class SimulationScheduleDefaultsResponse(BaseModel):
    school_id: str
    period_start: date
    period_end: date
    closures: list[SimulationBaselineClosure]


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

# `RoomSimulationInput` defines the input simulation parameters for a specific school room.
class RoomSimulationInput(RoomSimulationConfig):
    room_id: str

# A period during which the school is fully closed (no occupancy, lighting, or HVAC).
class ScheduleHoliday(BaseModel):
    name: str | None = None
    start_date: date
    end_date: date | None = None

    @model_validator(mode="after")
    def validate_period(self):
        if self.end_date is None:
            self.end_date = self.start_date
        if self.end_date < self.start_date:
            raise ValueError("holiday end_date must be on or after start_date")
        if (self.end_date - self.start_date) > timedelta(days=365):
            raise ValueError("holiday period may not exceed 366 days")
        return self


# A Saturday/Sunday on which the school operates with its weekday schedules.
class ScheduleWeekendEvent(BaseModel):
    name: str | None = None
    event_date: date

    @model_validator(mode="after")
    def validate_weekend(self):
        if self.event_date.weekday() < 5:
            raise ValueError("weekend event_date must fall on a Saturday or Sunday")
        return self


# A built-in IDF holiday date reopened for weekday-scheduled rooms in this run.
class ScheduleOpeningDate(BaseModel):
    name: str | None = None
    opening_date: date


# User-defined per-run schedule configuration. Opening overrides are filtered
# by each room's operating-schedule capability during IDF preprocessing.
class SimulationScheduleInput(BaseModel):
    school_day_start_time: time | None = None
    school_day_end_time: time | None = None
    holidays: list[ScheduleHoliday] = Field(default_factory=list)
    weekend_events: list[ScheduleWeekendEvent] = Field(default_factory=list)
    opening_dates: list[ScheduleOpeningDate] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_school_day_window(self):
        start = self.school_day_start_time
        end = self.school_day_end_time
        if (start is None) != (end is None):
            raise ValueError(
                "school_day_start_time and school_day_end_time must be provided together"
            )
        if start is not None and end is not None:
            if start.tzinfo is not None or end.tzinfo is not None:
                raise ValueError("school day times must not include a timezone")
            if start < time(6, 0) or end > time(20, 0):
                raise ValueError("school day must fall between 06:00 and 20:00")
            start_minutes = start.hour * 60 + start.minute
            end_minutes = end.hour * 60 + end.minute
            if end_minutes - start_minutes < 60:
                raise ValueError(
                    "school_day_end_time must be at least one hour after school_day_start_time"
                )
            if (
                start.minute != 0
                or end.minute != 0
                or start.second != 0
                or end.second != 0
                or start.microsecond != 0
                or end.microsecond != 0
            ):
                raise ValueError(
                    "school day times must fall on whole-hour boundaries (e.g. 08:00, 14:00)"
                )

        sorted_holidays = sorted(
            self.holidays,
            key=lambda holiday: (holiday.start_date, holiday.end_date),
        )
        for previous, current in zip(sorted_holidays, sorted_holidays[1:]):
            if current.start_date <= previous.end_date:
                raise ValueError("school closure periods may not overlap")

        weekend_dates = [event.event_date for event in self.weekend_events]
        if len(weekend_dates) != len(set(weekend_dates)):
            raise ValueError("weekend opening dates must be unique")

        for event_date in weekend_dates:
            if any(
                holiday.start_date <= event_date <= holiday.end_date
                for holiday in sorted_holidays
            ):
                raise ValueError(
                    "a date may not be both a school closure and a weekend opening"
                )

        opening_dates = [opening.opening_date for opening in self.opening_dates]
        if len(opening_dates) != len(set(opening_dates)):
            raise ValueError("school opening override dates must be unique")

        for opening_date in opening_dates:
            if any(
                holiday.start_date <= opening_date <= holiday.end_date
                for holiday in sorted_holidays
            ):
                raise ValueError(
                    "a date may not be both a user-defined school closure and an opening override"
                )
            if opening_date in weekend_dates:
                raise ValueError(
                    "a date may not be both a weekend opening and a school opening override"
                )
        return self

    def has_custom_school_hours(self) -> bool:
        return self.school_day_start_time is not None and self.school_day_end_time is not None

    def is_empty(self) -> bool:
        return (
            not self.has_custom_school_hours()
            and not self.holidays
            and not self.weekend_events
            and not self.opening_dates
        )


# Schema for SimulationInput, which defines the input parameters for a simulation.
class SimulationInput(BaseModel):
    school_id: str
    rooms: list[RoomSimulationInput] = Field(min_length=1)
    simulation_horizon: SimulationHorizon
    forecast_start_date: date | None = None
    custom_start_date: date | None = None
    custom_end_date: date | None = None
    schedule: SimulationScheduleInput | None = None

    @model_validator(mode="after")
    def validate_custom_period(self):
        room_ids = [room.room_id for room in self.rooms]
        if len(room_ids) != len(set(room_ids)):
            raise ValueError("rooms must contain unique room_id values")

        if self.simulation_horizon == "custom":
            if self.custom_start_date is None or self.custom_end_date is None:
                raise ValueError(
                    "custom_start_date and custom_end_date are required when simulation_horizon is 'custom'"
                )
            if self.custom_start_date > self.custom_end_date:
                raise ValueError("custom_end_date must be on or after custom_start_date")
        elif self.custom_start_date is not None or self.custom_end_date is not None:
            raise ValueError(
                "custom_start_date and custom_end_date may only be set when simulation_horizon is 'custom'"
            )
        if (
            self.simulation_horizon not in {"forecast_1d", "forecast_7d"}
            and self.forecast_start_date is not None
        ):
            raise ValueError(
                "forecast_start_date may only be set for forecast horizons"
            )
        return self


class SimulationAIQuestionRequest(BaseModel):
    question: str = Field(min_length=3, max_length=500)

    @model_validator(mode="after")
    def normalize_question(self):
        self.question = self.question.strip()
        if len(self.question) < 3:
            raise ValueError("question must contain at least three characters")
        return self


IndoorEnvironmentAIMode = Literal["realtime", "historical"]


class IndoorEnvironmentAIQuestionRequest(BaseModel):
    question: str = Field(min_length=3, max_length=500)
    school_id: str = Field(min_length=1, max_length=100)
    device_id: str = Field(min_length=1, max_length=100)
    mode: IndoorEnvironmentAIMode
    start_date: date | None = None
    end_date: date | None = None

    @model_validator(mode="after")
    def normalize_and_validate_scope(self):
        self.question = self.question.strip()
        self.school_id = self.school_id.strip()
        self.device_id = self.device_id.strip()
        if len(self.question) < 3:
            raise ValueError("question must contain at least three characters")
        if not self.school_id or not self.device_id:
            raise ValueError("school_id and device_id are required")

        if self.mode == "historical":
            if self.start_date is None or self.end_date is None:
                raise ValueError(
                    "start_date and end_date are required in historical mode"
                )
            if self.start_date > self.end_date:
                raise ValueError("end_date must be on or after start_date")
            if (self.end_date - self.start_date).days > 365:
                raise ValueError("historical range cannot exceed 366 days")
        elif self.start_date is not None or self.end_date is not None:
            raise ValueError(
                "start_date and end_date may only be set in historical mode"
            )
        return self


class DayAheadSimulationRequest(BaseModel):
    school_id: str
    target_date: date | None = Field(
        default=None,
        description=(
            "Athens-local D+1 date. Omission is temporarily supported and resolves to tomorrow."
        ),
    )
    room_ids: list[str] | None = None
    occupancy_multiplier: float = Field(default=1.0, ge=0.0, le=5.0)
    heating_setpoint_offset_c: float = Field(default=0.0, ge=-5.0, le=5.0)
    cooling_setpoint_offset_c: float = Field(default=0.0, ge=-5.0, le=5.0)

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
    latest_event_time: datetime
    readings: dict[str, OverviewReading]


class DeviceHistoryBucketItem(BaseModel):
    device_id: str
    event_time: datetime
    measurements: dict[str, OverviewReading]


class DeviceHistoryResponse(BaseModel):
    device_id: str
    count: int
    items: list[DeviceHistoryBucketItem]


IAQPolicyMetric = Literal["co2", "pm25"]
IAQThresholdWindow = Literal["short", "long"]
IAQPeriodId = Literal["1m", "1h", "24h", "7d", "14d"]


class IAQThresholdPolicy(BaseModel):
    value: float
    period_ids: list[IAQPeriodId]


class IAQMetricPolicy(BaseModel):
    label: str
    unit: str
    thresholds: dict[IAQThresholdWindow, IAQThresholdPolicy]


class IAQPolicyResponse(BaseModel):
    version: str
    metrics: dict[IAQPolicyMetric, IAQMetricPolicy]


class ShellyPro3emPhaseEnergyWh(BaseModel):
    """Estimated energy (Wh) per phase and sum, from bucketed average active power."""

    a: float
    b: float
    c: float
    total: float


class ShellyPro3emEnergyEstimateResponse(BaseModel):
    """Response shape from the Shelly Pro 3EM optimized /energy endpoint (power integration)."""

    device_id: str
    start: datetime
    end: datetime
    bucket_minutes: int
    energy_wh: ShellyPro3emPhaseEnergyWh


class RoomHourlyDeviceEnergy(BaseModel):
    """Per-series contribution inside one hourly bucket (plug = one series; Pro 3EM = one per phase in this room)."""

    device_id: str
    label: str
    energy_wh: float
    phase: Literal["a", "b", "c"] | None = None


class RoomHourlyEnergyPoint(BaseModel):
    """One hour bucket: total plus per-device slices for stacked charts."""

    window_start: str
    window_end: str
    energy_wh_total: float
    by_device: list[RoomHourlyDeviceEnergy] = Field(default_factory=list)


class RoomHourlyEnergyResponse(BaseModel):
    """Aggregated hourly Shelly energy for all meters in a school room."""

    room_key: str
    start: str | None = None
    end: str | None = None
    working_only: bool
    emissions_factor_kg_per_kwh: float = Field(ge=0.0)
    emissions_factor_source: str
    emissions_factor_reference_year: int | None = None
    emissions_factor_version: str
    device_ids: list[str]
    count: int
    points: list[RoomHourlyEnergyPoint]


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


class SolarActivePowerPoint(BaseModel):
    timestamp: str
    value: float | None


class SolarDayAheadForecastPoint(BaseModel):
    timestamp: str
    hour: int
    predicted_power_kw: float


class SolarDayAheadForecast(BaseModel):
    forecast_date: str
    count: int
    items: list[SolarDayAheadForecastPoint]


class SolarDayAheadForecastRangeResponse(BaseModel):
    collection: str
    source: Literal["postgres"]
    start_date: str
    end_date: str
    count: int
    forecasts: list[SolarDayAheadForecast]


class SolarActivePowerResponse(BaseModel):
    collection: str
    source: Literal["postgres"] | None = None
    latest_observed_at: str | None = None
    start_date: str
    end_date: str
    count: int
    points: list[SolarActivePowerPoint]


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
