# Prompt Merger - FastAPI Service
FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

EXPOSE 8100

HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8100/health')" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8100"]
