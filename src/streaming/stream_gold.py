import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, max, min, count

# 1. Carregar chaves de ambiente
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env")

STORAGE_ACCOUNT_NAME = "stiotanalyticsrbgsprod"
STORAGE_ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_ACCESS_KEY")

# 2. Caminhos da Arquitetura Medalhão na Azure
SILVER_PATH    = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/telemetry/"
GOLD_PATH      = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold/telemetry_agg_1m/"
GOLD_CHECKPOINT= f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold/telemetry_agg_1m_checkpoint/"

# 3. Forçar amarração de rede no Windows
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("IoT-Streaming-Gold") \
    .master("local[2]") \
    .config("spark.jars.packages", "io.delta:delta-spark_4.1_2.13:4.1.0,org.apache.hadoop:hadoop-azure:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.network.timeout", "800s") \
    .config("spark.local.dir", "/tmp/spark_local_gold") \
    .config("spark.hadoop.hadoop.tmp.dir", "/tmp/hadoop_tmp_gold") \
    .config(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY) \
    .getOrCreate()

print("✅ Spark iniciado para a Camada Gold.")

# 4. LEITURA STREAMING (Lendo da tabela Delta Silver)
df_silver_stream = spark.readStream \
    .format("delta") \
    .load(SILVER_PATH)

# 5. AGREGAÇÃO EM TEMPO REAL (Janela de 1 minuto com tolerância de 2 minutos para atrasos)
df_gold_aggregated = df_silver_stream \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute").alias("time_window"),
        col("device_id")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        max("temperature").alias("max_temperature"),
        min("temperature").alias("min_temperature"),
        avg("pressure").alias("avg_pressure"),
        count("device_id").alias("total_events")
    ) \
    .select(
        col("time_window.start").alias("window_start"),
        col("time_window.end").alias("window_end"),
        col("device_id"),
        col("avg_temperature"),
        col("max_temperature"),
        col("min_temperature"),
        col("avg_pressure"),
        col("total_events")
    )

# 6. ESCRITA STREAMING NA GOLD (Modo Complete ou Update é necessário para agregações)
print("📡 Agregando dados em tempo real e gravando na Gold...")

query_gold = df_gold_aggregated.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", GOLD_CHECKPOINT) \
    .start(GOLD_PATH)

query_gold.awaitTermination()