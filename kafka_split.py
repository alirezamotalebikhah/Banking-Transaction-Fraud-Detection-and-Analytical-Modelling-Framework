from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, when
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaSplitStream") \
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
        .withColumn("has_null", 
            when(col("transaction_id").isNull() | 
                 col("customer_id").isNull() | 
                 col("transaction_type").isNull() | 
                 col("amount").isNull() | 
                 col("currency").isNull() | 
                 col("timestamp").isNull(), True).otherwise(False))
    
    clean_df = final_df.filter(col("has_null") == False) \
        .select("transaction_id", "customer_id", "transaction_type", 
                "amount", "currency", "txn_timestamp", "txn_date", "txn_hour") \
        .select(to_json(struct("*")).alias("value"))
    
    invalid_df = final_df.filter(col("has_null") == True) \
        .select("transaction_id", "customer_id", "transaction_type", 
                "amount", "currency", "txn_timestamp", "txn_date", "txn_hour") \
        .select(to_json(struct("*")).alias("value"))
    
    print("Data transformation completed")
    
    print("\nWriting clean data to Kafka topic: bank_transaction_clean")
    clean_query = clean_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "bank_transaction_clean") \
        .option("checkpointLocation", "hdfs://namenode:8020/tmp/checkpoint_clean/") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("Writing invalid data to Kafka topic: bank_transaction_invalid")
    invalid_query = invalid_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "bank_transaction_invalid") \
        .option("checkpointLocation", "hdfs://namenode:8020/tmp/checkpoint_invalid/") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("=" * 60)
    print("Streaming started")
    print("=" * 60)
    print("\nData is being split to two topics:")
    print("1. bank_transaction_clean")
    print("2. bank_transaction_invalid")
    print("Press Ctrl+C to stop...\n")
    
    spark.streams.awaitAnyTermination()

except Exception as e:
    print(f"\nError: {str(e)}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("\nSpark Session closed")