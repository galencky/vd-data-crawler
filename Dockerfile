# ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

# avoid byte-code files & enable log flush
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# optional: configure defaults for the pipeline
ENV BASE_DIR=/data \
    TIMEZONE=Asia/Taipei \
    MAX_DL_WORKERS=2 \
    MAX_PARSE_WORKERS=2

# install minimal build deps, then purge apt cache
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# project code
COPY main.py .

# mount point declared for clarity
VOLUME ["/data"]

ENTRYPOINT ["python", "main.py"]
# you can still append CLI flags at `docker run` time
