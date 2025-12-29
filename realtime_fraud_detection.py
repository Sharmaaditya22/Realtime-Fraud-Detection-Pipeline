from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import sys
import redis
from loguru import logger
import json

# --- 1. SETUP ENVIRONMENT ---
os.environ['HADOOP_HOME'] = "C:\\hadoop"
sys.path.append("C:\\hadoop\\bin")
os.environ['PATH'] += os.pathsep + "C:\\hadoop\\bin"

# Locate JARs
current_dir = os.getcwd()
jar_folder = os.path.join(current_dir, "jars")
delta_core = os.path.join(jar_folder, "delta-core_2.12-2.4.0.jar")
delta_storage = os.path.join(jar_folder, "delta-storage-2.4.0.jar")

if not os.path.exists(delta_core):
    logger.error(f"Error: Could not find {delta_core}")
    sys.exit(1)

# --- 2. THE SINK FUNCTION ---
def write_to_sinks(batch_df, batch_id):
    deltalake_path = os.path.join(os.getcwd(), 'lakehouse')

    if batch_df.isEmpty():
        return
    
    # Persist the batch since we use it twice (Delta + Redis)
    batch_df.persist()

    # --- A. Write to Delta Lake ---
    logger.info(f"Batch {batch_id}: Writing {batch_df.count()} records to Delta Lake...")
    (batch_df.write
     .format("delta")
     .mode("append")
     .save(deltalake_path))
    
    # --- B. Write to Redis ---
    # FIX 1: Use 'batch_df', NOT 'final_df'
    fraud_df = batch_df.filter(col('is_fraud') == 'Yes')

    if not fraud_df.isEmpty():
        count = fraud_df.count() # FIX 2: Define count here
        logger.info(f"Batch {batch_id}: Found {count} fraud records! Pushing to Redis...")
        
        try:
            # Collect to Driver (Safe for Windows/Local mode)
            fraud_rows = fraud_df.collect()
            
            # Connect to Redis
            r = redis.Redis(host='localhost', port=6379, db=0)
            
            for row in fraud_rows:
                key = f"fraud:{row.user_id}"
                
                # Create dictionary
                fraud_data = {
                    'user_id': row.user_id,
                    'user_name': row.user_name,
                    'amount': row.amount,
                    'merchant': row.merchant,
                    'reason': row.Reason, # Ensures 'Reason' column exists in schema
                    'timestamp': str(row.ingest_time)
                }
                
                # Save to Redis (Expires in 1 min)
                r.setex(key, 60, json.dumps(fraud_data))
                
            r.close()
            logger.info(f"Successfully pushed {count} alerts to Redis.")
            
        except Exception as e:
            logger.error(f"Redis Write Failed: {e}")

    # Unpersist to free memory
    batch_df.unpersist()

# --- 3. MAIN PIPELINE ---
if __name__ == "__main__":

    logger.info("Starting Spark Session...")

    # FIX 3: Cleaned up duplicate configs
    spark = (SparkSession
             .builder
             .appName('FraudDetectorRealTime')
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
             .config("spark.jars", f"{delta_core},{delta_storage}")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.driver.host", "127.0.0.1")
             .master('local[*]')
             .getOrCreate())
    spark

    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark Session started.")

    # A. Read Kafka
    kafka_df = spark.readStream.format('kafka')\
        .option('kafka.bootstrap.servers', 'localhost:9092')\
        .option('subscribe', 'raw_transactions')\
        .option('startingOffsets', 'latest')\
        .load()

    # B. Define Schema
    kafka_df = kafka_df.withColumn('value', col('value').cast('string'))
    
    json_schema = StructType([
        StructField('transaction_id', StringType()),
        StructField('user_id', LongType()),
        StructField('user_name', StringType()),
        StructField('amount', DoubleType()),
        StructField('currency', StringType()),
        StructField('merchant', StringType()),
        StructField('location', StringType()),
        StructField('timestamp', DoubleType()),
    ])

    # C. Parse JSON
    parsed_df = kafka_df.withColumn('value_json', from_json(col('value'), json_schema)).select('value_json.*')
    parsed_df = parsed_df.withColumn('timestamp', to_timestamp((col('timestamp')).cast('long')))

    # --- NEW: ADD WATERMARK HERE ---
    # This handles late data (up to 5 minutes behind)
    parsed_df = parsed_df.withWatermark("timestamp", "5 minutes")

    # D. Enrich with Bad Merchants (Broadcast Join)
    bad_merchants_data = [
        ("Evil Corp", "High Risk"),
        ("Fake Store Ltd", "Banned"),
        ("Scam Hub", "Under Investigation")
    ]
    merchant_schema = ['merchant', 'risk_level']
    bad_merchants_df = spark.createDataFrame(data=bad_merchants_data, schema=merchant_schema)

    enriched_df = parsed_df.join(broadcast(bad_merchants_df), ['merchant'], 'left')

    # E. Apply Fraud Logic
    final_df = enriched_df.withColumn('is_fraud', 
                                      when(col('amount') > 10000, 'Yes')
                                      .when(col('risk_level').isNotNull(), 'Yes')
                                      .otherwise('No')
    ).withColumn('Reason', 
                 when(col('amount') > 10000, 'High Amount')
                 .when(col('risk_level').isNotNull(), 'Banned Merchant')
                 .otherwise('Normal')
    ).withColumn("ingest_time", current_timestamp())

    # F. Start Stream
    CHECKPOINT_PATH = os.path.join(os.getcwd(), 'checkpoints')
    
    logger.info("Pipeline initialized. Waiting for data...")
    query = (final_df.writeStream
             .foreachBatch(write_to_sinks)
             .outputMode("append")
             .option("checkpointLocation", CHECKPOINT_PATH)
             .trigger(processingTime="5 seconds")
             .start())

    query.awaitTermination()