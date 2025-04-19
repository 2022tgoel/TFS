#! /usr/bin/env python3

# There is some really jank behavior with the traces when a span starts in one tokio worker
# thread and ends in another worker thread.
# Post-process the traces so each span is in a single thread.

import json
import sys
from collections import defaultdict

def process_trace(input_file, output_file):
    # Stack to keep track of open spans per name
    span_stacks = defaultdict(list)
    # Store all events
    events = []
    
    # Read and parse the trace file
    with open(input_file, 'r') as f:
        for line in f:
            # Skip empty lines
            if not line.strip():
                continue
            # Remove trailing comma if present
            if line.strip().endswith(','):
                line = line.strip()[:-1]
            try:
                event = json.loads(line)
                events.append(event)
            except json.JSONDecodeError:
                print(f"Warning: Skipping malformed line: {line}")
    
    # Process events to match B and E pairs
    for event in events:
        if event['ph'] == 'B':
            # For begin events, push to stack
            key = event['name']
            span_stacks[key].append(event)
        elif event['ph'] == 'E':
            # For end events, pop matching begin event and fix tid
            key = event['name']
            if span_stacks[key]:
                matching_begin = span_stacks[key].pop()
                # Make end event tid match begin event tid
                event['tid'] = matching_begin['tid']

    # Write processed events back to file
    with open(output_file, 'w') as f:
        f.write('[\n')
        for i, event in enumerate(events):
            f.write(json.dumps(event))
            # Add comma for all but last event
            if i < len(events) - 1:
                f.write(',\n')
            else:
                f.write('\n')
        f.write(']')

def main():
    if len(sys.argv) != 2:
        print("Usage: python fix_trace.py <trace_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = input_file.rsplit('.', 1)[0] + '_fixed.' + input_file.rsplit('.', 1)[1]
    
    try:
        process_trace(input_file, output_file)
        print(f"Processed trace written to {output_file}")
    except Exception as e:
        print(f"Error processing trace: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 