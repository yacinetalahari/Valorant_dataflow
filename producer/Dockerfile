# Base image with Python 3.11 slim (smaller size)
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy dependencies file and install them
# This is where pandas and kafka-python are installed
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the configuration file and the main script
COPY config.py producer.py .

# Command to run the producer script when the container starts
CMD ["python", "producer.py"]