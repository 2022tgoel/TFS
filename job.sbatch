#!/bin/bash
#SBATCH -n 2
#SBATCH -N 2
#SBATCH -p mit_normal

# Define the port you want to check
PORT=5555

# Function to check if a port is available
check_port() {
    # Use ss to check if the port is in use
    if ss -ltn | grep ":$PORT" > /dev/null; then
        echo "Port $PORT is already in use."
        return 1  # Port is in use
    else
        echo "Port $PORT is available."
        return 0  # Port is available
    fi
}

# Call the function and wait until the port is available
check_port
while [ $? -ne 0 ]; do
    echo "Waiting for port $PORT to be available..."
    sleep 5  # Wait 5 seconds before checking again
    check_port
done

RUST_BACKTRACE=1 ./target/debug/tfs