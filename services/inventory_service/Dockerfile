# Use Python 3.11 slim
FROM python:3.11-slim

WORKDIR /app

# Copy Python dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire service
COPY . .

# Expose Flask port
EXPOSE 5000

# Default command to run Flask
CMD ["flask", "run", "--host=0.0.0.0"]
