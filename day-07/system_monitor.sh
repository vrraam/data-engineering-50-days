#!/bin/bash
# System Health Monitor for macOS

echo "=== System Health Report ==="
echo "Generated: $(date)"
echo

# System information
echo "=== System Information ==="
echo "Hostname: $(hostname)"
echo "Uptime: $(uptime)"
echo

# Memory usage
echo "=== Memory Usage ==="
vm_stat | head -5

# Disk usage
echo -e "\n=== Disk Usage ==="
df -h | head -5

# Top processes by CPU
echo -e "\n=== Top 5 CPU Processes ==="
ps aux | sort -k3 -nr | head -6

# Network connections
echo -e "\n=== Network Status ==="
netstat -tuln | head -5

echo -e "\n=== Report Complete ==="
