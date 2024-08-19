## Sub Triage Logs

Small experimental tool to triage Substrate-based testing stacks and group logs by common patterns.

## Usage

### warn-err

```bash
cargo run -- warn-err --address "address-url" --start-time "2024-03-29T16:00:00Z" --end-time "2024-03-30T16:00:00Z"
```

This command groups warnings and errors by their message and counts the number of occurrences.

### panics

```bash
cargo run -- panics --address "address-url"  --start-time "2024-03-29T20:00:00Z" --end-time "2024-03-30T20:00:00Z"
```

This command checks the testing stack for panics that occurred between the specified start and end times.  
The queries are chunked in 1 hour intervals to avoid timeouts. Each query is retried at most 3 times.

### warp-time

```bash
cargo run -- warp-time --file ../2024-08-19_11-56-49-2e15a0f92ab6b0454eec87ef632946290c9909dc-litep2p.txt
```

This command computes the warp time from the logs of a running substrate node.

Example output:

Phase | Time
 -|-
Warp  | 521.999999816s
State | 83.000000298s
Total | 605.000000114s
