FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY day_ahead_forecast.py .
COPY fetch_open_meteo_forecast.py .
COPY pv_features_rf_operational_20260806.pkl .
COPY pv_forecasting_model_rf_operational_20260806.pkl .
COPY pv_model_manifest_rf_operational_20260806.json .

ENTRYPOINT ["python", "-u", "day_ahead_forecast.py"]
