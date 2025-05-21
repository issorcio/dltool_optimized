# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# Copy the requirements file into the container at /app
COPY requirements.txt ./

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script into the container at /app
# Make sure your script is named dltool_optimized.py in the same directory as the Dockerfile
COPY dltool_optimized.py ./

# Make port 80 available to the world outside this container (if needed, though this script is CLI)
# EXPOSE 80 

# Define environment variable (if needed)
# ENV NAME World

# Run dltool_optimized.py when the container launches
ENTRYPOINT ["python", "dltool_optimized.py"]

# Default command arguments can be specified with CMD
# For example, to show help by default if no other args are given:
# CMD ["--help"]
