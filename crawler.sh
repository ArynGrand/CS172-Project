#!/bin/bash

# ---------------------------------------
# Reddit Crawler Launcher - CS172 Project
# ---------------------------------------
# Usage:
# ./crawler.sh <seed_file> <num_pages> <hops_away> <output_dir> <num_procs> <timeout> [--debug]

# Check argument count
if [ "$#" -lt 6 ]; then
  echo "Usage: $0 <seed_file> <num_pages> <hops_away> <output_dir> <num_procs> <timeout> [--debug]"
  exit 1
fi

# Assign arguments
SEED_FILE=$1
NUM_PAGES=$2
HOPS_AWAY=$3
OUTPUT_DIR=$4
NUM_PROCS=$5
TIMEOUT=$6
DEBUG=${7:-""}  # optional

# Activate virtual environment
if [ -d "venv" ]; then
  source venv/bin/activate
else
  echo "Virtual environment not found. Run 'python3 -m venv venv' and activate it."
  exit 1
fi

pip install -r requirements.txt

python3 crawler.py \
  --seed_file "$SEED_FILE" \
  --num_pages "$NUM_PAGES" \
  --hops_away "$HOPS_AWAY" \
  --output_dir "$OUTPUT_DIR" \
  --num_procs "$NUM_PROCS" \
  --timeout "$TIMEOUT" \
  $DEBUG