from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, current_timestamp, lit
from pyspark.sql.types import *
import requests
from requests.exceptions import RequestException

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Spark Session created - Fraud Detection System Starting...")
print("=" * 80)


schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("transaction_type", StringType()),
    StructField("amount", StringType()),
    StructField("currency", StringType()),
    StructField("timestamp", StringType()),
])


def send_batch_to_clickhouse(rows, fraud_type_name):
    if not rows:
        return

    
    data = []
    for row in rows:
        data.append({
            "transaction_id": row.transaction_id,
            "customer_id": row.customer_id,
            "transaction_type": row.transaction_type,
            "amount": float(row.amount) if row.amount is not None else 0.0,
            "currency": row.currency or "IRR",
            "fraud_type": row.fraud_type,
            "fraud_timestamp": row.fraud_timestamp
        })

    try:
        response = requests.post(
            "http://default:password@clickhouse:8123/",
            params={"query": "INSERT INTO fraud_alerts FORMAT JSONEachRow"},
            json=data,
            timeout=15
        )
        if response.status_code == 200:
            print(f"{fraud_type_name} | Sent {len(data)} alerts → ClickHouse")
        else:
            print(f"ClickHouse Error {response.status_code}: {response.text}")
    except RequestException as e:
        print(f"ClickHouse Connection failed (will retry next batch): {e}")
    except Exception as e:
        print(f"Unexpected error sending to ClickHouse: {e}")


print("\nStarting stream for INVALID data → HDFS + Hive")

invalid_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bank_transaction_invalid") \
    .option("startingOffsets", "earliest") \
    .load()

invalid_final = invalid_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*") \
    .withColumn("txn_date", col("timestamp").substr(1, 10)) \
    .withColumn("txn_hour", col("timestamp").substr(12, 2)) \
    .withColumn("amount", col("amount").cast("double"))

hdfs_query = invalid_final.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/user/hive/warehouse/bank_transaction_invalid") \
    .option("checkpointLocation", "hdfs://namenode:8020/tmp/checkpoint_invalid_hdfs/") \
    .partitionBy("txn_date", "txn_hour") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Invalid data stream → HDFS started")


print("\nStarting stream for CLEAN data → Fraud Detection → ClickHouse")

clean_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bank_transaction_clean") \
    .option("startingOffsets", "earliest") \
    .load()

clean_final = clean_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*") \
    .withColumn("txn_timestamp", col("timestamp")) \
    .withColumn("txn_date", col("timestamp").substr(1, 10)) \
    .withColumn("amount", col("amount").cast("double"))


high_amount_fraud = clean_final.filter(col("amount") > 100_000_000) \
    .withColumn("fraud_type", lit("HIGH_AMOUNT")) \
    .withColumn("fraud_timestamp", col("txn_timestamp")) \
    .select("transaction_id", "customer_id", "transaction_type", "amount", "currency", "fraud_type", "fraud_timestamp")

def write_high_amount_batch(batch_df, batch_id):
    rows = batch_df.collect()
    send_batch_to_clickhouse(rows, "HIGH_AMOUNT")

high_amount_query = high_amount_fraud.writeStream \
    .foreachBatch(write_high_amount_batch) \
    .outputMode("append") \
    .trigger(processingTime="20 seconds") \
    .start()


high_count_fraud = clean_final.groupBy("customer_id", "txn_date") \
    .agg(count("*").alias("cnt")) \
    .filter(col("cnt") > 100) \
    .withColumn("fraud_type", lit("HIGH_TRANSACTION_COUNT")) \
    .withColumn("transaction_id", lit("DAILY_AGG")) \
    .withColumn("amount", lit(0.0)) \
    .withColumn("currency", lit("IRR")) \
    .withColumn("transaction_type", lit("MULTIPLE_TXNS")) \
    .withColumn("fraud_timestamp", current_timestamp().cast("string")) \
    .select("transaction_id", "customer_id", "transaction_type", "amount", "currency", "fraud_type", "fraud_timestamp")

def write_high_count_batch(batch_df, batch_id):
    rows = batch_df.collect()
    send_batch_to_clickhouse(rows, "HIGH_COUNT")

high_count_query = high_count_fraud.writeStream \
    .foreachBatch(write_high_count_batch) \
    .outputMode("update") \
    .trigger(processingTime="60 seconds") \
    .start()

print("=" * 80)
print("ALL STREAMS STARTED SUCCESSFULLY!")
print("1. Invalid → HDFS (partitioned)")
print("2. Clean → Fraud Detection → ClickHouse (HTTP batch)")
print("Running... Press Ctrl+C to stop")
print("=" * 80)

spark.streams.awaitAnyTermination()