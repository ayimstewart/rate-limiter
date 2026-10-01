FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first: this layer is rebuilt only when requirements.txt changes.
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src ./src

RUN useradd --system --no-create-home --uid 10001 app
USER app

EXPOSE 8000

# 200 only while the storage backend answers; python is already in the image, curl is not.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"]

# WEB_CONCURRENCY=N runs N worker processes. With STORAGE_BACKEND=memory each worker keeps its
# own counters (the limit is effectively multiplied by N); use redis if you scale out.
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
