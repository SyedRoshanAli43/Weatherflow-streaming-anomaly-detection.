FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY ../requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install additional dependencies for Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Copy application code
COPY .. /app/

# Expose ports
EXPOSE 8050
EXPOSE 8080
EXPOSE 5000

# Set environment variables
ENV PYTHONPATH=/app

# Entry point
CMD ["bash", "-c", "python /app/dashboard/app.py"]
