import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Carrega as credenciais do .env
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env")

STORAGE_ACCOUNT_NAME = "stiotanalyticsrbgsprod"
STORAGE_ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_ACCESS_KEY")
GOLD_PATH            = f"abfss://datalake@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold/telemetry_agg_1m/"

# 2. Configura e inicia o Spark Session
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
spark = SparkSession.builder \
    .appName("IoT-Read-Gold") \
    .master("local[2]") \
    .config("spark.jars.packages", "io.delta:delta-spark_4.1_2.13:4.1.0,org.apache.hadoop:hadoop-azure:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY) \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.port", "7079") \
    .config("spark.blockManager.port", "6062") \
    .config("spark.network.timeout", "800s") \
    .config("spark.local.dir", "/tmp/spark_local_read_gold") \
    .config("spark.hadoop.hadoop.tmp.dir", "/tmp/hadoop_tmp_read_gold") \
    .getOrCreate()

# 3. LEITURA BATCH DA TABELA DELTA GOLD
print("\n📖 Lendo agregações estratégicas da Camada Gold (Métricas de 1 min)...")
df_gold = spark.read.format("delta").load(GOLD_PATH)

# 4. EXIBIÇÃO DOS RESULTADOS ANALÍTICOS
print("\n📊 Estrutura dos Dados Agregados (Schema):")
df_gold.printSchema()

print("\n📈 Métricas de Telemetria por Janela de Tempo (Últimos 20 blocos calculados):")
# Ordenamos pelas janelas mais recentes e pelo ID do dispositivo para facilitar a leitura
df_gold \
    .orderBy(col("window_start").desc(), col("device_id")) \
    .show(20, truncate=False)

print(f"Total de janelas analíticas consolidadas na Gold: {df_gold.count()}")