#!/bin/bash

# Check if a number of iterations was provided.
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <number_of_iterations>"
    exit 1
fi

ITERATIONS=$1
LOG_DIR="crash_logs"

# Create the log directory if it doesn't exist.
mkdir -p "$LOG_DIR"

for (( i = 1; i <= ITERATIONS; i++ )); do
    # Generate a random seed.
    SEED=$RANDOM
    echo "Iteration $i with seed: $SEED"
    
    TEMP_OUTPUT="temp_output_${SEED}.log"
    EXIT_FILE="/tmp/exit_code_${SEED}.txt"

    # Run the command inside bash and capture its exit status.
    script -q -c "bash -c '/home/hesham/xinu/test/build/test $SEED; ec=\$?; echo \$ec > $EXIT_FILE; exit \$ec'" "$TEMP_OUTPUT"

    # Read the captured exit status.
    if [ -f "$EXIT_FILE" ]; then
         EXIT_STATUS=$(cat "$EXIT_FILE")
         rm "$EXIT_FILE"
    else
         EXIT_STATUS=0
    fi

    if [ "$EXIT_STATUS" -ne 0 ]; then
        OUTPUT_FILE="${LOG_DIR}/${SEED}.log"
        cp "$TEMP_OUTPUT" "$OUTPUT_FILE"
        echo "Program crashed with seed $SEED (exit status $EXIT_STATUS). Output saved to $OUTPUT_FILE"
    fi

    # Always remove the temporary output file.
    rm -f "$TEMP_OUTPUT"
done
