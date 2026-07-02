import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# 1. Carrega as variáveis de ambiente do arquivo .env
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Configurações do ecossistema local Windows
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import requests
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

STORAGE_ACCOUNT_NAME = "stiotanalyticsrbgsprod"
STORAGE_ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_ACCESS_KEY")
WEBHOOK_URL          = os.getenv("WEBHOOK_ALERTS_URL")

BRONZE_PATH  = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/telemetry/"
SILVER_PATH  = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/telemetry/"
CHECKPOINT   = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/telemetry_checkpoint_v2/"

spark = SparkSession.builder \
    .appName("IoT-Silver-Streaming-Processor") \
    .master("local[2]") \
    .config("spark.jars.packages", "io.delta:delta-spark_4.1_2.13:4.1.0,org.apache.hadoop:hadoop-azure:3.3.4,org.apache.hadoop:hadoop-common:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.network.timeout", "800s") \
    .config("spark.local.dir", "/tmp/spark_local_silver") \
    .config("spark.hadoop.hadoop.tmp.dir", "/tmp/hadoop_tmp_silver") \
    .config(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY) \
    .getOrCreate()    

print("🤖 Iniciando consumo da camada Bronze em tempo real...")

    # Leitura dos dados brutos em streaming da Bronze
df_bronze_stream = spark.readStream \
    .format("delta") \
    .load(BRONZE_PATH)

# 2. Schema do JSON bruto unificado (Conforme o transformations.py)
def get_iot_schema() -> StructType:
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("reading_value", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

# 3. Função pura de transformação (Camada Silver)
def transform_iot_data(df_raw: DataFrame) -> DataFrame:
    schema = get_iot_schema()
    
    # Decodifica o JSON do campo 'raw_payload' e preserva metadados de ingestão
    df_parsed = df_raw.withColumn("parsed_data", from_json(col("raw_payload"), schema)) \
                      .select("parsed_data.*", "ingested_at_hub")
    
    # Regra de Negócio: Identifica anomalias (status crítico ou leitura > 100)
    df_silver = df_parsed.withColumn(
        "is_anomaly",
        when((col("status") == "critical") | (col("reading_value") > 100.0), True).otherwise(False)
    )
    
    return df_silver.select(
        col("timestamp").cast("timestamp").alias("timestamp"), 
        col("device_id"), 
        col("sensor_type"), 
        col("reading_value"), 
        col("status"),
        col("is_anomaly"),
        col("ingested_at_hub")
    )

# 4. Sistema de Alertas via Webhook do Discord
def send_discord_alert(anomaly_percentage, total_rows):
    if not WEBHOOK_URL:
        return
    
    payload = {
        "embeds": [{
            "title": "🚨 Alerta de Anomalia na Esteira - Camada Silver",
            "color": 15158332,  # Cor Vermelha
            "description": f"Foi detectada uma alta taxa de anomalias nos sensores IoT industriais.",
            "fields": [
                {"name": "Percentual de Anomalias", "value": f"{anomaly_percentage:.2f}%", "inline": True},
                {"name": "Total de Registros no Lote", "value": str(total_rows), "inline": True},
                {"name": "Status do Pipeline", "value": "⚠️ Investigação Necessária", "inline": False}
            ],
            "footer": {"text": "Sistema de Monitoramento IoT Real-time"}
        }]
    }
    
    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"Erro ao enviar alerta para o Discord: {e}")

# 5. Processamento por lote (Micro-batch) para persistência e validação de regras
def process_micro_batch(df_batch: DataFrame, batch_id: int):
    total_rows = df_batch.count()
    
    if total_rows > 0:
        # Calcula o percentual de anomalias no micro-batch atual
        anomaly_count = df_batch.filter(col("is_anomaly") == True).count()
        anomaly_percentage = (anomaly_count / total_rows) * 100
        
        print(f"📥 Processando Batch ID: {batch_id} | Total Linhas: {total_rows} | Anomalias: {anomaly_percentage:.2f}%")
        
        # Dispara o webhook se a regra de 5% de anomalias for atingida
        if anomaly_percentage >= 5.0:
            send_discord_alert(anomaly_percentage, total_rows)
        
        # Grava os dados transformados na camada Silver (Formato Parquet/Delta)
        df_batch.write \
            .format("delta") \
            .mode("append") \
            .save(SILVER_PATH)

# 6. Inicialização do Stream Core
def start_streaming():   
    # Aplica as regras de negócio do transformations.py
    df_transformed_stream = transform_iot_data(df_bronze_stream)

    # Direciona o fluxo para o processador de micro-batch e checkpoints
    query = df_transformed_stream.writeStream \
        .foreachBatch(process_micro_batch) \
        .option("checkpointLocation", CHECKPOINT) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()