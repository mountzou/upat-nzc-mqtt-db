FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fusionsolar.py pipeline.py persistence.py main.py ./

ENTRYPOINT ["python", "-u", "main.py"]
