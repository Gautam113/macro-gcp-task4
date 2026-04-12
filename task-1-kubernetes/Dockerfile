FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY grid_producer.py .
COPY power_consumption.csv .

# Non-root user for security
RUN useradd -m producer
USER producer

EXPOSE 8080

CMD ["python", "grid_producer.py"]
