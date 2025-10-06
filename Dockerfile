# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# --- INSTALL FFMPEG ---
# Update package lists and install ffmpeg
RUN apt-get update && apt-get install -y ffmpeg

# Copy your Python requirements file
COPY requirements.txt .

# Install your Python libraries
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your bot's code
COPY . .

# Command to run your bot when the container starts
CMD ["python", "main.py"]
