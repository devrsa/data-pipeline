FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY config/ ./config/
COPY dags/ ./dags/
COPY data/ ./data/

ENV PYTHONPATH=/app/src

EXPOSE 8080

CMD ["python", "-m", "src.etl_pipeline"]