import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, max, min, count

# Carregar chaves de ambiente
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Forçar amarração de rede no Windows (Seu ambiente local de desenvolvimento)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

STORAGE_ACCOUNT_NAME = "stiotanalyticsrbgsprod"
STORAGE_ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_ACCESS_KEY")

# Caminhos da Arquitetura Medalhão na Azure
SILVER_PATH    = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/telemetry/"
GOLD_PATH      = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold/telemetry_agg_1m/"
GOLD_CHECKPOINT= f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold/telemetry_agg_1m_checkpoint/"

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

print("✅ Spark iniciado para a Camada Gold com suporte ao novo esquema.")

# LEITURA STREAMING (Lendo da camada Silver - Certifique-se de usar o formato correto Parquet/Delta)
df_silver_stream = spark.readStream \
    .format("delta") \
    .load(SILVER_PATH)

# AGREGAÇÃO EM TEMPO REAL ADAPTADA (Modelo Chave-Valor)
# Incluímos o 'sensor_type' no groupBy para segmentar as métricas de forma limpa!
df_gold_aggregated = df_silver_stream \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute").alias("time_window"),
        col("device_id"),
        col("sensor_type")
    ) \
    .agg(
        avg("reading_value").alias("val_medio"),
        max("reading_value").alias("val_maximo"),
        min("reading_value").alias("val_minimo"),
        count("device_id").alias("total_eventos")
    ) \
    .select(
        col("time_window.start").alias("window_start"),
        col("time_window.end").alias("window_end"),
        col("device_id"),
        col("sensor_type"),
        col("val_medio"),
        col("val_maximo"),
        col("val_minimo"),
        col("total_eventos")
    )

# ESCRITA STREAMING NA GOLD
print("📡 Agregando dados (sensor_type/reading_value) em tempo real e gravando na Gold...")

query_gold = df_gold_aggregated.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", GOLD_CHECKPOINT) \
    .start(GOLD_PATH)

query_gold.awaitTermination()