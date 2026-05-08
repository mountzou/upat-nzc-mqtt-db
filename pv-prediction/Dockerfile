FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY day_ahead_forecast.py .
COPY fetch_open_meteo_forecast.py .
COPY pv_features.pkl .
COPY pv_forecasting_model.pkl .

ENTRYPOINT ["python", "-u", "day_ahead_forecast.py"]
