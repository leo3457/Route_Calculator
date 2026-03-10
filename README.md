# Route Calculator Performance Optimization

## Executive Summary

The route calculation engine (`process_golden_df`) processes millions of rows of CAN bus telemetry to generate deterministic driving routes. Initially, the engine suffered from severe CPU bottlenecks, requiring **~45–50 seconds** to process a batch of **~180 trucks**.

By refactoring the calculation loop to leverage **Pandas vectorization** and **`itertuples()`**, we eliminated massive Python-level memory allocation and interpreter overhead.

**Result:**  
Processing time for a standard 180-truck batch dropped from **~48 seconds to ~0.7 seconds** — a **64× speed improvement** — enabling the API to return `200 OK` responses almost instantly.

---

## The Bottleneck: Scalar Iteration with `iterrows()`

The original implementation processed telemetry using `df.iterrows()`:

### OLD IMPLEMENTATION (Slow)
`for _, row in df.iterrows():
    odo_mi = row['odometer'] * 0.000621371
    time_gap = (row['time'] - prev_time).total_seconds()`

This approach had two critical flaws:

1. Scalar Math in Python

- Each mathematical operation was executed row by row in Python. As a dynamically typed language, Python must repeatedly:

    - Perform type checks

    - Allocate memory

    - Dispatch operations

- This overhead occurred 500,000+ times per run, severely degrading performance.

2. Pandas Series Overhead

- iterrows() constructs a full Pandas Series object for every row. This resulted in:

    - Excessive memory allocation

    - CPU thrashing

    - High overhead just to access simple fields (e.g., timestamps)

### NEW IMPLEMENTATION (Vectorized)

- Optimization 1: Vectorization (C/C++ Execution Path)

    - Vectorization replaces sequential Python loops with bulk array operations executed in optimized C/C++ code.

    - Instead of computing values inside the loop, all heavy math was moved outside the loop and applied to entire columns at once.

 1. Convert odometer values to miles
`df['odo_mi'] = df['odometer'] * METERS_TO_MILES`

 2. Compute time deltas for the entire dataframe
`df['time_gap'] = (
    df['time']
    .diff()
    .dt.total_seconds()
    .fillna(0.0)
)`

    - By the time iteration begins, 100% of the computationally expensive math is already complete, executed at the hardware level.

### NEW IMPLEMENTATION (itertuples)

- Optimization 2: Lightweight Iteration with itertuples()

    - With math removed from the loop, the remaining bottleneck was the overhead of iterrows(). This was replaced with df.itertuples().

`for row in df.itertuples():
    # Dot-notation access (near-zero overhead)
    t = row.time
    odo_mi = row.odo_mi
    time_gap = row.time_gap`
    
- Why this works better

    - itertuples() yields lightweight, read-only NamedTuples

    - No Pandas Series objects are created

    - Dot-notation avoids dictionary-style lookups

    - Memory allocation inside the loop is minimized

- Additionally, object copying (e.g., prev_row = row.copy()) was replaced with primitive assignments:

    - prev_time = t
    - prev_odo = odo_mi

    - This completely eliminates object-creation overhead during iteration.

### Performance Benchmark Results

Benchmarks were run against identical local PostgreSQL datasets using time.perf_counter().

- Test Scenario 1: Historical Batch (183 Trucks)

    - Old Engine: 41.230 seconds

    - New Engine: 0.638 seconds

    - Improvement: 64.6× faster

- Test Scenario 2: Real-Time Batch (186 Trucks)

    - Old Engine: 48.653 seconds

    - New Engine: 0.761 seconds

    - Improvement: 63.9× faster

### Future Scalability: Multi-Processing Ready

- All database queries have been moved outside the calculation loop, fully resolving the N+1 query problem. As a result, the current architecture is now embarrassingly parallel.

- If fleet size grows to the point where a single vectorized loop becomes a bottleneck (e.g., 5,000+ trucks), the system is already prepared to scale via:

    - concurrent.futures.ProcessPoolExecutor

- Full CPU core utilization

- Near-linear performance gains per core
