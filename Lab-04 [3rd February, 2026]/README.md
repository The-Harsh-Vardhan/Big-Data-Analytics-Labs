# Lab 04 — Spark Structured Streaming

**Date:** 3rd February, 2026

## Objective

Learn real-time stream processing using Apache Spark's Structured Streaming API — reading streaming data, performing aggregations, writing output to files, and handling late data with watermarking.

## Tasks

| # | Task | File | Description |
|---|------|------|-------------|
| 1 | Streaming DataFrame | `task1_stream_df.py` | Create a streaming DataFrame from text files |
| 2 | Streaming Word Count | `task2_stream_wordcount.py` | Real-time word count on streaming data |
| 3 | Streaming to File | `task3_stream_wordcount_to_file.py` | Write streaming word count results to CSV |
| 4 | Event Filtering | `task4_stream_filter.py` | Filter streaming lines containing "spark" |
| 6 | Watermarking | `task6_watermarking.py` | Handle late-arriving data with watermarks and window aggregation |

### Additional Variants

| File | Description |
|------|-------------|
| `task2_stream_wordcount_single_run.py` | One-shot trigger (`trigger(once=True)`) |
| `task2_stream_wordcount_single_terminal.py` | Uses `rate` source for self-contained streaming |
| `task3_stream_wordcount_single_terminal.py` | `foreachBatch` output to CSV |
| `task3_wordcount_batch.py` | Batch-mode word count for comparison |

## Prerequisites

- Apache Spark 3.4.2
- Python 3 with PySpark

## How to Run

### Continuous streaming (Tasks 1, 2, 4)

```bash
# Terminal 1: Start the streaming job
cd "Lab-04 [3rd February, 2026]"
spark-submit task2_stream_wordcount.py

# Terminal 2: Add files to the streaming_input/ directory
echo "big data spark" >> streaming_input/data.txt
```

The streaming job watches the `streaming_input/` folder and processes new files as they appear.

### One-shot execution (Tasks 3, 6)

```bash
# Processes existing files once and exits
spark-submit task3_stream_wordcount_single_terminal.py
spark-submit task6_watermarking.py
```

## Directory Structure

```
Lab-04 [3rd February, 2026]/
├── task1_stream_df.py
├── task2_stream_wordcount.py
├── task2_stream_wordcount_single_run.py
├── task2_stream_wordcount_single_terminal.py
├── task3_stream_wordcount_single_terminal.py
├── task3_stream_wordcount_to_file.py
├── task3_wordcount_batch.py
├── task4_stream_filter.py
├── task6_watermarking.py
├── streaming_input/            # Input data directory
│   ├── data.txt
│   ├── data1.txt
│   ├── task3_data.txt
│   ├── task4_data.txt
│   └── task6_data.txt
└── checkpoint_task3/           # Spark checkpoint directory
```

## Key Concepts

### Structured Streaming
- Treats a live data stream as an unbounded table that keeps growing
- Uses the same DataFrame/SQL API as batch processing
- Supports three output modes:
  - **Append** — only new rows added to the result
  - **Complete** — entire result table is output every trigger
  - **Update** — only changed rows are output

### Watermarking
- Defines how long the system waits for late-arriving data
- `withWatermark("event_time", "2 minutes")` allows data up to 2 minutes late
- Enables the engine to discard old state and keep memory bounded

### Windowed Aggregation
- Groups streaming events into fixed time windows
- `window(col("event_time"), "1 minute")` creates 1-minute tumbling windows

## Sample Watermark Input (`task6_data.txt`)

```
2026-01-01 10:00:00,login
2026-01-01 10:00:30,click
2026-01-01 10:01:00,login
2026-01-01 10:01:15,click
```
