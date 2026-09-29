FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY model.pkl .
COPY model_manifest.json .

ENTRYPOINT ["python", "-u", "main.py"]
