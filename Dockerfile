FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /app /data

USER appuser

CMD ["/bin/sh", "-lc", "if [ \"${UVICORN_RELOAD:-0}\" = \"1\" ]; then exec uvicorn app:app --host 0.0.0.0 --port 8000 --reload; else exec uvicorn app:app --host 0.0.0.0 --port 8000; fi"]
