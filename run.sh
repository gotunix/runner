#!/bin/bash

# Exit on any error
set -e

# Change directory to script's location
cd "$(dirname "$0")"

echo "======================================"
echo " Setting up Background Job Runner   "
echo "======================================"

if [ ! -d "venv" ]; then
    echo "1. Creating Python virtual environment (venv)..."
    python3 -m venv venv
else
    echo "1. Virtual environment already exists."
fi

echo "2. Activating virtual environment..."
source venv/bin/activate

echo "3. Installing requirements..."
pip install -r requirements.txt > /dev/null

echo "4. Giving execution permissions to runner script..."
chmod +x runner.py

echo "======================================"
echo " Executing Task Runner                "
echo "======================================"

#python runner.py --config tasks.yaml --verbose-all
python runner.py --config tasks.yaml --verbose
