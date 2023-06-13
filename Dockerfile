# define base image as python slim-buster.
FROM python:3.7-slim-buster as base

# Set the working directory
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and download the browsers
RUN pip install playwright
RUN playwright install

# Copy the app
COPY *.py .

# Change the ownership of our apps to non-root user
RUN adduser --uid 1000 --disabled-password --gecos "" appuser && chown -R appuser /app

# Switch to non-root user
USER appuser

# Expose port 8000
EXPOSE 8000

# Run the app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
