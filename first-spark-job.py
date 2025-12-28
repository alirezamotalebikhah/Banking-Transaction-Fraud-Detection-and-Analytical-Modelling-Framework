from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaToHive") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Spark Session created")
print("=" * 60)

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("transaction_type", StringType()),
    StructField("amount", StringType()),
    StructField("currency", StringType()),
    StructField("timestamp", StringType()),
])

try:
    print("\nConnecting to Kafka...")
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "bank-transactions") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 5000) \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("Kafka connection successful")
    
    json_df = df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), schema).alias("data")) \
                .select("data.*")
    
    final_df = json_df \
        .withColumn("txn_timestamp", col("timestamp")) \
        .withColumn("txn_date", col("timestamp").substr(1, 10)) \
        .withColumn("txn_hour", col("timestamp").substr(12, 2)) \
        .withColumn("amount", col("amount").cast(DoubleType())) \
        .select("transaction_id", "customer_id", "transaction_type", 
                "amount", "currency", "txn_timestamp", "txn_date", "txn_hour")
    
    print("Data transformation completed")
    
    print("\nWriting data to HDFS...")
    print("Path: /user/hive/warehouse/transaction_raw/")
    print("Format: Parquet")
    print("Partition: txn_date, txn_hour\n")
    
    query = final_df.writeStream \
        .format("parquet") \
        .option("path", "hdfs://namenode:8020/user/hive/warehouse/transaction_raw") \
        .option("checkpointLocation", "hdfs://namenode:8020/tmp/checkpoint_kafka/") \
        .partitionBy("txn_date", "txn_hour") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("=" * 60)
    print("Streaming started")
    print("=" * 60)
    print("\nData is being written to HDFS...")
    print("Press Ctrl+C to stop...\n")
    
    query.awaitTermination()

except Exception as e:
    print(f"\nError: {str(e)}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("\nSpark Session closed")