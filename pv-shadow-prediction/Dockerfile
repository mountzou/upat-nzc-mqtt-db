FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py model_registry.json ./

ENTRYPOINT ["python", "-u", "main.py"]
