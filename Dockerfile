# define base image as python slim-buster.
FROM mcr.microsoft.com/playwright/python:v1.35.0-jammy

# Set the working directory
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app
COPY *.py .

# Expose port 8000
EXPOSE 8000

# Run the app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
