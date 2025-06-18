#!/bin/bash
# Advanced Log Analysis Script

LOG_FILE="$1"

# Check if file is provided
if [ -z "$LOG_FILE" ]; then
    echo "Usage: $0 <log-file>"
    echo "Example: $0 sample-logs.txt"
    exit 1
fi

# Check if file exists
if [ ! -f "$LOG_FILE" ]; then
    echo "Error: File $LOG_FILE not found!"
    exit 1
fi

echo "=== Log Analysis Report ==="
echo "File: $LOG_FILE"
echo "Generated: $(date)"
echo

# Basic statistics
total_lines=$(wc -l < "$LOG_FILE")
echo "Total log entries: $total_lines"

# Log level analysis
echo -e "\n=== Log Level Distribution ==="
awk '{print $3}' "$LOG_FILE" | sort | uniq -c | sort -nr

# Error analysis
error_count=$(grep -c "ERROR" "$LOG_FILE")
echo -e "\n=== Error Analysis ==="
echo "Total errors: $error_count"

if [ $error_count -gt 0 ]; then
    echo "Error details:"
    grep "ERROR" "$LOG_FILE"
fi

# Time range analysis
echo -e "\n=== Time Range ==="
first_entry=$(head -1 "$LOG_FILE" | awk '{print $1, $2}')
last_entry=$(tail -1 "$LOG_FILE" | awk '{print $1, $2}')
echo "First entry: $first_entry"
echo "Last entry: $last_entry"

echo -e "\n=== Analysis Complete ==="
