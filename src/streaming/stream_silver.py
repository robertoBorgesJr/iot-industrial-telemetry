import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp

# 1. Carregar chaves de ambiente
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env")

STORAGE_ACCOUNT_NAME = "stiotanalyticsrbgsprod"
STORAGE_ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_ACCESS_KEY")

# 2. Caminhos da Arquitetura Medalhão na Azure
BRONZE_PATH      = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/telemetry/"
SILVER_PATH      = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/telemetry/"
SILVER_CHECKPOINT= f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/telemetry_checkpoint/"

# 3. Forçar amarração de rede no Windows
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("IoT-Streaming-Silver") \
    .master("local[2]") \
    .config("spark.jars.packages", "io.delta:delta-spark_4.1_2.13:4.1.0,org.apache.hadoop:hadoop-azure:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.network.timeout", "800s") \
    .config("spark.local.dir", "/tmp/spark_local_silver") \
    .config("spark.hadoop.hadoop.tmp.dir", "/tmp/hadoop_tmp_silver") \
    .config("fs.azure.account.key.stiotanalyticsrbgsprod.dfs.core.windows.net", STORAGE_ACCOUNT_KEY) \
    .getOrCreate()

print("✅ Spark iniciado para a Camada Silver.")

# 4. LEITURA STREAMING (Lendo da tabela Delta Bronze)
df_bronze_stream = spark.readStream \
    .format("delta") \
    .load(BRONZE_PATH)

# Definindo o Schema de extração
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
IOT_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("status", StringType(), True)
])

# 5. TRANSFORMAÇÃO E LIMPEZA (Parseando o JSON e ajustando tipos)
df_silver_transformed = df_bronze_stream \
    .withColumn("parsed_data", from_json(col("raw_payload"), IOT_SCHEMA)) \
    .select(
        col("parsed_data.device_id").alias("device_id"),
        # Convertendo o timestamp UNIX do sensor (segundos) para Timestamp real do Spark
        to_timestamp(from_unixtime(col("parsed_data.timestamp"))).alias("event_time"),
        col("parsed_data.temperature").cast("float").alias("temperature"),
        col("parsed_data.pressure").cast("float").alias("pressure"),
        col("parsed_data.vibration").cast("float").alias("vibration"),
        col("parsed_data.status").alias("status"),
        col("ingested_at_hub"),
        col("processed_at_spark").alias("bronze_processed_at")
    ) \
    .filter(col("device_id").isNotNull()) # Regra de qualidade básica

# 6. ESCRITA STREAMING NA SILVER (Gravando como Delta limpo)
print("📡 Processando dados da Bronze e gravando na Silver...")

query_silver = df_silver_transformed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", SILVER_CHECKPOINT) \
    .start(SILVER_PATH)

query_silver.awaitTermination()