# Use the official Python image as a base
FROM python:3.9

# Set up a working directory
WORKDIR /app

# Copy requirements.txt into the container
COPY requirements.txt ./

# Install all dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code into the container
COPY train_sarima.py .

# Set the command to execute the SARIMA script
CMD ["python", "train_sarima.py"]
