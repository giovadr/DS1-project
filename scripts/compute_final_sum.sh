#/bin/sh

LOG_FILE="$1"

# print sum for each server
cat "$LOG_FILE" | grep 'FINAL SUM'

# print total sum
cat "$LOG_FILE" | grep 'FINAL SUM' | cut -d ' ' -f 4 | sed 's/^/+/' | tr -d '\n' | cut -c 2- | bc

