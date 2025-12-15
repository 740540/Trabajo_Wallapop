#!/bin/bash
# Wallapop Agent Runner

cd "$(dirname "$0")"

# Set environment
export ES_HOST="http://192.168.153.2:9200"
export PYTHONUNBUFFERED=1

# Run agent
echo "Starting Wallapop Agent at $(date)"
python3 wallapop_agent.py

echo "Agent finished at $(date)"
