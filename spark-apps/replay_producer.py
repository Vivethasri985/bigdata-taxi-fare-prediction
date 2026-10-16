"""
SurgeSense Phase 12 - Historical Replay Producer
------------------------------------------------
Purpose:
    Replay held-out March 2026 taxi trips as simulated streaming
    micro-batches for Spark Structured Streaming.

Source:
    hdfs://namenode:9000/surgesense/processed/split/test

Output:
    hdfs://namenode:9000/surgesense/streaming/incoming/batch_XXXXX/

Behavior:
    1. Read held-out test partition from HDFS.
    2. Filter for March 2026 events.
    3. Retain exact 21 raw predictor + event time fields (zero ML preprocessing).
    4. Order by pickup_datetime ascending.
    5. Emit micro-batches of 200–250 records approximately every 3 seconds.
"""

import time
import random
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
SOURCE_PATH = "hdfs://namenode:9000/surgesense/processed/split/test"
INCOMING_BASE = "hdfs://namenode:9000/surgesense/streaming/incoming"
MIN_BATCH_SIZE = 200
MAX_BATCH_SIZE = 250
REPLAY_INTERVAL_SECONDS = 3

# Allow optional record limit override via CLI (default: 20,000 for initial test)
MAX_REPLAY_RECORDS = int(sys.argv[1]) if len(sys.argv) > 1 else 20000

def main():
    print("=" * 70)
    print("SurgeSense Phase 12: Historical Replay Producer")
    print(f"Target Replay Records: {MAX_REPLAY_RECORDS}")
    print(f"Batch Size: {MIN_BATCH_SIZE} - {MAX_BATCH_SIZE} records / batch")
    print(f"Cadence: ~{REPLAY_INTERVAL_SECONDS} seconds / batch")
    print("=" * 70)

    spark = SparkSession.builder \
        .appName("SurgeSense-Replay-Producer") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Load test partition
    print(f"\n[1] Reading test dataset from: {SOURCE_PATH}")
    test_df = spark.read.parquet(SOURCE_PATH)

    # 2. Select March 2026 records
    print("[2] Filtering for held-out March 2026 trips...")
    march_df = test_df.filter(
        (col("pickup_year") == 2026) & (col("pickup_month") == 3)
    )

    # 3. Retain exact raw fields required by feature encoder & streaming engine
    raw_fields = [
        "pickup_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
        "passenger_count",
        "trip_distance",
        "payment_type",
        "trip_duration_min",
        "average_speed_mph",
        "pickup_year",
        "pickup_month",
        "pickup_hour",
        "day_of_week",
        "is_weekend",
        "temperature_2m",
        "precipitation",
        "wind_speed_10m",
        "weather_code",
        "fare_amount"
    ]

    selected_df = march_df.select(*raw_fields)

    # 4. Order strictly by pickup_datetime
    print(f"[3] Ordering records by pickup_datetime and collecting {MAX_REPLAY_RECORDS} rows...")
    ordered_df = selected_df.orderBy("pickup_datetime").limit(MAX_REPLAY_RECORDS)
    
    # Collect to driver as pure Spark Row objects (no pandas dependency)
    schema = ordered_df.schema
    rows = ordered_df.collect()
    total_loaded = len(rows)
    print(f"    Loaded {total_loaded} ordered trips into driver memory.")

    print("\n[4] Commencing micro-batch streaming replay...")
    print(f"Destination: {INCOMING_BASE}/batch_XXXXX/\n")

    current_idx = 0
    batch_num = 1
    total_emitted = 0
    start_time = time.time()

    while True:
        batch_size = random.randint(MIN_BATCH_SIZE, MAX_BATCH_SIZE)
        end_idx = min(current_idx + batch_size, total_loaded)
        
        batch_slice = rows[current_idx:end_idx]
        actual_size = len(batch_slice)

        # Create batch DataFrame using original schema and write as JSON directory
        batch_df = spark.createDataFrame(batch_slice, schema=schema)
        batch_dir = f"{INCOMING_BASE}/batch_{batch_num:05d}"
        
        batch_write_start = time.time()
        batch_df.coalesce(1).write.mode("overwrite").json(batch_dir)
        write_duration = time.time() - batch_write_start

        latest_timestamp = str(batch_slice[-1]["pickup_datetime"])
        total_emitted += actual_size

        print(f"  [Batch {batch_num:05d}] Emitted {actual_size:3d} trips | "
              f"Simulated Event Time: {latest_timestamp} | "
              f"Total: {total_emitted:5d}/{total_loaded} | "
              f"Write Time: {write_duration:.2f}s")

        current_idx = end_idx
        if current_idx >= total_loaded:
            print("  [replay] Reached end of partition, looping for continuous 24/7 stream...")
            current_idx = 0
        batch_num += 1

        # Pause to simulate controlled arriving stream cadence
        time.sleep(REPLAY_INTERVAL_SECONDS)

    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"Replay complete! Total events emitted: {total_emitted}")
    print(f"Total batches: {batch_num - 1} | Elapsed time: {elapsed:.1f}s")
    print(f"Average throughput: {total_emitted / elapsed:.1f} events/second")
    print("=" * 70)

    spark.stop()

if __name__ == "__main__":
    main()
