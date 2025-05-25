# Base image (choose one appropriate for your Spark version and environment)
# For GCP DataProc, you might use a DataProc base image or a Debian/Ubuntu image with Java/Python.
# For on-prem, it might be different.
FROM python:3.9-slim

WORKDIR /app

# Copy shared utilities (adjust path if necessary, this assumes Docker context is project root)
COPY ./shared_utils /app/shared_utils
COPY ./config /app/config

# Copy job-specific files
COPY ./sample_subject_area/sample_job/ /app/sample_subject_area/sample_job/

# Install dependencies
# First, install global/shared dependencies if any (managed outside this Dockerfile for now)
# Then, install job-specific dependencies
RUN if [ -f /app/sample_subject_area/sample_job/requirements.txt ]; then pip install --no-cache-dir -r /app/sample_subject_area/sample_job/requirements.txt; fi

# Set PYTHONPATH to include shared_utils and the job's directory
ENV PYTHONPATH="/app:/app/shared_utils:/app/sample_subject_area/sample_job"

# Entry point for the Spark application
# This is just a placeholder; actual submission might be via spark-submit
# CMD ["spark-submit", "/app/sample_subject_area/sample_job/app.py"]
CMD ["python", "/app/sample_subject_area/sample_job/app.py"] 
# Using python directly for app.py to run its main block for now, 
# as spark-submit requires more setup.
