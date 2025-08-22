#!/bin/bash
set -e

echo "Starting ChromeDriver..."
chromedriver --port=9515 --whitelisted-ips="" &
CHROMEDRIVER_PID=$!

echo "Waiting for ChromeDriver to initialize..."
sleep 2

if [ ! -f /data/input/*.xlsx ]; then
  echo "Error: No Excel files found in /data/input"
  exit 1
fi

EXCEL_FILE=$(ls /data/input/*.xlsx | head -1)
echo "Using Excel file: $EXCEL_FILE"

export WEBDRIVER_URL="http://localhost:9515"
export RUST_LOG=${RUST_LOG:-info}

CMD="/app/reddit_crawler_rs --excel $EXCEL_FILE --db /data/output/reddit.duckdb"

if [ "${HEADLESS}" != "false" ]; then
  CMD="$CMD --headless"
fi

CMD="$CMD --workers ${WORKERS:-2}"
CMD="$CMD --rpm ${RPM:-20}"

echo "Starting Reddit crawler with: $CMD $@"
$CMD "$@"

kill $CHROMEDRIVER_PID