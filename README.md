"""
Data Engineering Zoomcamp - Module 3 (BigQuery)

This file contains:
- The SQL queries used to answer Questions 1–9
- The final answers written explicitly as comments
"""

from google.cloud import bigquery

client = bigquery.Client()

PROJECT_ID = "your-project-id"
DATASET_ID = "ny_taxi"
GCS_URI = "gs://your-bucket/yellow_tripdata_2024-*.parquet"

# -------------------------------------------------------------------
# External Table
# -------------------------------------------------------------------
external_table_id = f"{PROJECT_ID}.{DATASET_ID}.yellow_taxi_external"

external_config = bigquery.ExternalConfig("PARQUET")
external_config.source_uris = [GCS_URI]

external_table = bigquery.Table(external_table_id)
external_table.external_data_configuration = external_config

client.create_table(external_table, exists_ok=True)

# -------------------------------------------------------------------
# Materialized Table
# -------------------------------------------------------------------
materialized_table_id = f"{PROJECT_ID}.{DATASET_ID}.yellow_taxi_materialized"

client.query(f"""
CREATE OR REPLACE TABLE `{materialized_table_id}` AS
SELECT *
FROM `{external_table_id}`;
""").result()

# ===================================================================
# Question 1
# What is the count of records for the 2024 Yellow Taxi Data?
# ANSWER: 20,332,093
# ===================================================================
client.query(f"""
SELECT COUNT(*) AS total_records
FROM `{materialized_table_id}`;
""").result()

# ===================================================================
# Question 2
# Estimated bytes read:
# External table: 0 MB
# Materialized table: ~155.12 MB
# ===================================================================
# (Observed directly in BigQuery UI)

# ===================================================================
# Question 3
# Why are bytes different?
# ANSWER:
# BigQuery is columnar and scans only the columns needed by the query.
# ===================================================================

# ===================================================================
# Question 4
# How many records have fare_amount = 0?
# ANSWER: 546,578
# ===================================================================
client.query(f"""
SELECT COUNT(*) AS zero_fare_count
FROM `{materialized_table_id}`
WHERE fare_amount = 0;
""").result()

# ===================================================================
# Question 5
# Best optimization strategy
# ANSWER:
# Partition by tpep_dropoff_datetime and Cluster on VendorID
# ===================================================================

optimized_table_id = f"{PROJECT_ID}.{DATASET_ID}.yellow_taxi_optimized"

client.query(f"""
CREATE OR REPLACE TABLE `{optimized_table_id}`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `{materialized_table_id}`;
""").result()

# ===================================================================
# Question 6
# Distinct VendorIDs between 2024-03-01 and 2024-03-15
# ANSWER:
# Non-partitioned table: ~310.24 MB
# Partitioned table: ~26.84 MB
# ===================================================================

client.query(f"""
SELECT DISTINCT VendorID
FROM `{materialized_table_id}`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
""").result()

client.query(f"""
SELECT DISTINCT VendorID
FROM `{optimized_table_id}`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
""").result()

# ===================================================================
# Question 7
# Where is data stored for External Tables?
# ANSWER: GCP Bucket
# ===================================================================

# ===================================================================
# Question 8
# Is it best practice to always cluster data?
# ANSWER: False
# ===================================================================

# ===================================================================
# Question 9 (Not graded)
# SELECT COUNT(*) from materialized table
# Estimated bytes read: ~155 MB
#
# WHY:
# COUNT(*) requires scanning all rows.
# No filters or partitions → full table scan.
# ===================================================================

job_q9 = client.query(f"""
SELECT COUNT(*) AS total_records
FROM `{materialized_table_id}`;
""")

job_q9.result()

print(
    f"Question 9 - Estimated bytes read: "
    f"{job_q9.total_bytes_processed / (1024 * 1024):.2f} MB"
)
