FROM python:3.11-slim

# Install system dependencies including ffmpeg for animated stickers
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Google Cloud Run dynamically assigns a PORT environment variable
ENV PORT=8080
EXPOSE $PORT

# Start the FastAPI application
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}
