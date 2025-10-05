# Dockerfile
FROM node:24

# Install Python, AWS CLI, and other dependencies
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3.11-venv \
    python3-pip \
    curl \
    unzip \
    gzip \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files
COPY frontend/ ./frontend/
COPY backend/ ./backend/
COPY README.md ./

# FRONTEND SETUP
WORKDIR /app/frontend

# Install frontend dependencies
RUN npm install

# DATA DOWNLOAD AND EXTRACTION
WORKDIR /app/backend

# Create data directory
RUN mkdir -p data/100k

# Download SynPUF dataset from AWS S3
RUN echo "Downloading required CSV files from AWS S3..." \
    && aws s3 cp s3://synpuf-omop/cmsdesynpuf100k/person.csv.gz ./data/100k/person.csv.gz --no-sign-request \
    && echo "Downloaded person.csv.gz" \
    && aws s3 cp s3://synpuf-omop/cmsdesynpuf100k/condition_occurrence.csv.gz ./data/100k/condition_occurrence.csv.gz --no-sign-request \
    && echo "Downloaded condition_occurrence.csv.gz" \
    && aws s3 cp s3://synpuf-omop/cmsdesynpuf100k/measurement.csv.gz ./data/100k/measurement.csv.gz --no-sign-request \
    && echo "Downloaded measurement.csv.gz" \
    && echo "Download complete!"

RUN echo "Extracting CSV files..." \
    && if [ -f data/100k/measurement.csv.gz ]; then gunzip data/100k/measurement.csv.gz; fi \
    && if [ -f data/100k/condition_occurrence.csv.gz ]; then gunzip data/100k/condition_occurrence.csv.gz; fi \
    && if [ -f data/100k/person.csv.gz ]; then gunzip data/100k/person.csv.gz; fi \
    && echo "Extraction complete!"


# BACKEND SETUP

# Create Python virtual environment
RUN python3.11 -m venv venv

# Install Python dependencies in venv
RUN ./venv/bin/pip install --upgrade pip \
    && ./venv/bin/pip install --no-cache-dir -r requirements.txt

# Run data generation script (generate lipid values,specifically around cohorts of diabetics)
RUN echo "Generating lipid panel values..." \
    && ./venv/bin/python data_gen.py ./data/100k/ 201820 \
    && echo "Data generation complete!"

# STARTUP SCRIPT
WORKDIR /app


# Create startup script to run both frontend and backend
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
echo "========================================"\n\
echo "Starting OMOP Visualizer"\n\
echo "========================================"\n\
echo ""\n\
\n\
# Start Flask backend\n\
echo "Starting Flask backend on port 5000..."\n\
cd /app/backend\n\
./venv/bin/python flaskApp.py &\n\
BACKEND_PID=$!\n\
\n\
# Wait for backend to initialize\n\
sleep 5\n\
\n\
# Start Vite dev server\n\
echo "Starting Vite dev server on port 3000..."\n\
cd /app/frontend\n\
npm run dev -- --host 0.0.0.0 &\n\
FRONTEND_PID=$!\n\
\n\
echo ""\n\
echo "========================================"\n\
echo "Application Ready!"\n\
echo "========================================"\n\
echo "Frontend: http://localhost:3000"\n\
echo "Backend:  http://localhost:5000"\n\
echo "========================================"\n\
echo ""\n\
\n\
# Function to handle shutdown\n\
cleanup() {\n\
    echo ""\n\
    echo "Shutting down..."\n\
    kill $BACKEND_PID $FRONTEND_PID 2>/dev/null\n\
    exit 0\n\
}\n\
\n\
# Trap SIGTERM and SIGINT\n\
trap cleanup SIGTERM SIGINT\n\
\n\
# Wait for any process to exit\n\
wait -n\n\
\n\
# Exit with status of process that exited first\n\
exit $?\n\
' > /app/start.sh && chmod +x /app/start.sh

# Expose ports
EXPOSE 3000 5000

# Set working directory back to app root
WORKDIR /app

# Run startup script
CMD ["/app/start.sh"]
