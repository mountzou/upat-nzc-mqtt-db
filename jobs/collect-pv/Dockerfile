FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api_limits.py fusionsolar.py processing.py database.py main.py ./

ENTRYPOINT ["python", "-u", "main.py"]
