import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Força o Spark e seus workers a usarem estritamente o localhost no Windows
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from chispa.dataframe_comparer import assert_df_equality
from src.streaming.stream_silver import transform_iot_data

@pytest.fixture(scope="session")
def spark_test_session():
    """Cria uma sessão Spark leve e local apenas para os testes unitários."""
    return SparkSession.builder \
        .master("local[*]") \
        .appName("pyspark-unit-tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .getOrCreate()

def test_transform_iot_data_detects_anomalies(spark_test_session):
    spark = spark_test_session

    # 1. Criamos os dados de entrada simulando a camada Bronze (raw_payload)
    input_data = [
        ('{"timestamp": "2026-06-26T14:00:00Z", "device_id": "MACH-101", "sensor_type": "temperature", "reading_value": 60.0, "status": "normal"}', "2026-06-26 14:01:00"),
        ('{"timestamp": "2026-06-26T14:02:00Z", "device_id": "MACH-102", "sensor_type": "temperature", "reading_value": 115.0, "status": "normal"}', "2026-06-26 14:03:00"), # Anomalia pelo valor > 100
        ('{"timestamp": "2026-06-26T14:04:00Z", "device_id": "MACH-103", "sensor_type": "vibration", "reading_value": 45.0, "status": "critical"}', "2026-06-26 14:05:00")   # Anomalia pelo status
    ]
    
    schema_input = ["raw_payload", "ingested_at_hub"]
    df_input = spark.createDataFrame(input_data, schema_input)

    # 2. Executamos a nossa função de transformação
    df_result = transform_iot_data(df_input)

    # 3. Criamos o DataFrame com o resultado ESPERADO para validação
    expected_data = [
        ("2026-06-26T14:00:00Z", "MACH-101", "temperature", 60.0, False),
        ("2026-06-26T14:02:00Z", "MACH-102", "temperature", 115.0, True),
        ("2026-06-26T14:04:00Z", "MACH-103", "vibration", 45.0, True)
    ]
    schema_expected = ["timestamp", "device_id", "sensor_type", "reading_value", "is_anomaly"]
    df_expected = spark.createDataFrame(expected_data, schema_expected)
    df_expected = spark.createDataFrame(expected_data, schema_expected).withColumn("timestamp", col("timestamp").cast("timestamp"))

    # 4. O Chispa compara as duas estruturas e valores
    assert_df_equality(df_result, df_expected, ignore_nullable=True)