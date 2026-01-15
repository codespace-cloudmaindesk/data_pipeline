FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev build-essential python3-dev && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
