import os
import sys
import requests
import json
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp, current_timestamp, lit

# 1. Carregar chaves de ambiente
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env")

STORAGE_ACCOUNT_NAME = "stiotanalyticsrbgsprod"
STORAGE_ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_ACCESS_KEY")
WEBHOOK_URL          = os.getenv("WEBHOOK_ALERTS_URL")

BRONZE_PATH  = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/telemetry/"
SILVER_PATH  = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/telemetry/"
DLQ_PATH     = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/dlq/"
CHECKPOINT   = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/telemetry_checkpoint_v2/"

# Configurações do ecossistema local Windows
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("IoT-Streaming-Silver-Alerts") \
    .master("local[2]") \
    .config("spark.jars.packages", "io.delta:delta-spark_4.1_2.13:4.1.0,org.apache.hadoop:hadoop-azure:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY) \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.network.timeout", "800s") \
    .config("spark.local.dir", "/tmp/spark_local_silver") \
    .config("spark.hadoop.hadoop.tmp.dir", "/tmp/hadoop_tmp_silver") \
    .config("fs.azure.account.key.stiotanalyticsrbgsprod.dfs.core.windows.net", STORAGE_ACCOUNT_KEY) \
    .getOrCreate()

# Schema de validação
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
IOT_SCHEMA = StructType([
    StructField("device_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("status", StringType(), True)
])

def enviar_alerta_webhook(mensagem):
    """ Envia uma notificação formatada para o canal de incidentes """
    if not WEBHOOK_URL or "sua_url_aqui" in WEBHOOK_URL:
        print(f"⚠️ [SIMULAÇÃO DE ALERTA]: {mensagem}")
        return
    
    payload = {"content": f"🚨 **SISTEMA DE MONITORAMENTO IoT** 🚨\n{mensagem}"}
    try:
        requests.post(WEBHOOK_URL, json=payload, timeout=5)
    except Exception as e:
        print(f"Falha ao enviar webhook: {e}")

def processar_lote_com_alertas(df_lote, batch_id):
    """ Processa o micro-batch atual, desviando erros para a DLQ e checando anomalias """
    if df_lote.isEmpty():
        return

    # Parse inicial do payload bruto
    df_parsed = df_lote.withColumn("parsed", from_json(col("raw_payload"), IOT_SCHEMA)) \
                       .select("parsed.*", "ingested_at_hub", "processed_at_spark")

    # Separar dados bons (Válidos) e ruins (DLQ)
    df_validos = df_parsed.filter(col("device_id").isNotNull() & (col("temperature") > -200))
    df_dlq = df_parsed.filter(col("device_id").isNull() | (col("temperature") <= -200)) \
                      .withColumn("rejected_at", current_timestamp()) \
                      .withColumn("reason", lit("Schema invalido ou dados de telemetria corrompidos"))

    # 1. Salva os dados limpos na Silver
    if not df_validos.isEmpty():
        df_validos.write.format("delta").mode("append").save(SILVER_PATH)
        
        # 🚨 ANÁLISE DE ANOMALIAS EM TEMPO REAL: Procurar superaquecimento imediato
        # Se a temperatura passar de 110 graus, disparar alerta
        df_criticos = df_validos.filter(col("temperature") > 110.0).collect()
        for linha in df_criticos:
            msg = f"Máquina `{linha['device_id']}` registrou Temperatura Crítica de **{linha['temperature']}°C**! Risco de quebra de componente."
            enviar_alerta_webhook(msg)

    # 2. Isola os dados ruins na DLQ para não quebrar a esteira
    if not df_dlq.isEmpty():
        print(f"❌ {df_dlq.count()} registros corrompidos enviados para a DLQ.")
        df_dlq.write.format("delta").mode("append").save(DLQ_PATH)
        enviar_alerta_webhook(f"Aviso: `{df_dlq.count()}` mensagens corrompidas foram descartadas e enviadas para a DLQ.")

# Leitura streaming contínua da Bronze
df_bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

print("📡 Iniciando esteira Silver com Monitoramento e DLQ ativo...")

# Execução usando o foreachBatch para aplicar a nossa função Python customizada por lote
query = df_bronze_stream.writeStream \
    .foreachBatch(processar_lote_com_alertas) \
    .option("checkpointLocation", CHECKPOINT) \
    .start()

query.awaitTermination()