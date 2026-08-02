FROM python:3.11-slim
# ffprobe is required by media-optimize.py (probe cache + reconcile verification)
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
COPY media-optimize.py /app/media-optimize.py
WORKDIR /app
ENTRYPOINT ["python3", "/app/media-optimize.py"]
