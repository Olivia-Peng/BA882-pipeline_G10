# Use a base image with Python 3.12
FROM python:3.12-slim

# Set the working directory
WORKDIR /app

# Install necessary dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the training script
COPY train.py .

# Run the training script
CMD ["python", "train.py"]
