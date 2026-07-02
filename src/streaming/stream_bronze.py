import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
#import socket
import tempfile

# carrega variáveis de ambiente do arquivo .env
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Configurações de acesso
EH_NAMESPACE   = "evhns-iot-analytics-rbgs-prod" 
EH_CONNECTION_STR = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")  

STORAGE_ACCOUNT_NAME = "stiotanalyticsrbgsprod" 
STORAGE_ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_ACCESS_KEY")  # ← Armazene a chave em variável de ambiente para segurança

# Caminho de destino na camada Bronze usando o protocolo ABFS (Azure Blob File System)
OUTPUT_PATH = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/telemetry/"
CHECKPOINT_PATH = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/telemetry_checkpoint/"

# Inicialização da spark session com os pacotes da azure e delta lake
# Definimos as versões dos pacotes compatíveis com o Spark 3.x
PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
    "io.delta:delta-spark_4.1_2.13:4.1.0", 
    "org.apache.hadoop:hadoop-azure:3.3.4"
]

# Força o ecossistema Python/Java do Spark a se comunicar APENAS via loopback local
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("IoT-Streaming-Bronze") \
    .master("local[2]") \
    .config("spark.jars.packages", ",".join(PACKAGES)) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.port", "7077") \
    .config("spark.blockManager.port", "6060") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.local.dir","/tmp/spark_local_bronze") \
    .config("spark.hadoop.hadoop.tmp.dir", "/tmp/hadoop_tmp_bronze") \
    .config(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY) \
    .getOrCreate()

print(f"✅ Spark {spark.version} iniciado com sucesso.")

# ==============================================================================
# LEITURA DO STREAMING (EVENT HUBS)
# ==============================================================================

BOOTSTRAP_SERVER = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
JAAS_CONFIG = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="$ConnectionString" '
    f'password="{EH_CONNECTION_STR}";'
)

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", JAAS_CONFIG) \
    .option("subscribe", "telemetry") \
    .option("startingOffsets", "earliest") \
    .load()

# ==============================================================================
# TRANSFORMAÇÃO BÁSICA (MANTENDO O DADO BRUTO)
# ==============================================================================
# No conceito de Medalhão, a Bronze armazena o dado bruto (body em binário) + metadados
df_bronze = df_stream.select(
    col("value").cast("string").alias("raw_payload"), # Converte os bytes do JSON para string
    col("timestamp").alias("ingested_at_hub"),        # Timestamp de quando chegou no Event Hub
    current_timestamp().alias("processed_at_spark")   # Nosso controle de auditoria
)

# ==============================================================================
# ESCRITA EM TEMPO REAL NO DELTA LAKE (AZURE)
# ==============================================================================
print(f"📡 Iniciando gravação em streaming na pasta Bronze do Data Lake...")

query = df_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start(OUTPUT_PATH)

query.awaitTermination()