FROM python:3.11-slim
# ffprobe required by media-optimize.py (probe cache + reconcile verification);
# ffsubsync + faster-whisper power the finalize overnight actions (srt re-sync,
# whisper transcription for missing/bad subtitles).
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg git \
    && rm -rf /var/lib/apt/lists/*
COPY media-optimize.py /app/media-optimize.py
COPY finalize.py /app/finalize.py
RUN pip install --no-cache-dir ffsubsync==0.5.1 faster-whisper
WORKDIR /app
ENTRYPOINT ["python3", "/app/media-optimize.py"]
