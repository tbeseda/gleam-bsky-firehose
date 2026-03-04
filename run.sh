#!/bin/bash
source /home/exedev/otp27/activate
cd /home/exedev/hello_gleam

# Start gleam in background
gleam run &
PID=$!

# Wait for startup
sleep 10

LAST_COUNT=0
STALL_CHECKS=0

# Health check loop
while true; do
    # Check HTTP is responding
    RESPONSE=$(curl -sf http://localhost:8000/ 2>/dev/null)
    if [ -z "$RESPONSE" ]; then
        echo "Health check failed: HTTP not responding"
        kill $PID 2>/dev/null
        exit 1
    fi
    
    # Extract post count
    COUNT=$(echo "$RESPONSE" | grep -oP 'Processed: \K[0-9]+')
    
    # Check if posts are being processed (after initial startup)
    if [ "$COUNT" = "$LAST_COUNT" ]; then
        STALL_CHECKS=$((STALL_CHECKS + 1))
        echo "Stall check $STALL_CHECKS: count still at $COUNT"
        if [ $STALL_CHECKS -ge 3 ]; then
            echo "Health check failed: no posts processed for 3 checks"
            kill $PID 2>/dev/null
            exit 1
        fi
    else
        STALL_CHECKS=0
        LAST_COUNT=$COUNT
    fi
    
    sleep 30
done
