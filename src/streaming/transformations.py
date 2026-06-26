from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Definimos o Schema do JSON bruto que vem do simulador
def get_iot_schema() -> StructType:
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("reading_value", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

# 2. Função pura de transformação (Camada Silver)
def transform_iot_data(df_raw: DataFrame) -> DataFrame:
    schema = get_iot_schema()
    
    # Decodifica o JSON do campo 'raw_payload' e aplica regras de negócio
    df_parsed = df_raw.withColumn("parsed_data", from_json(col("raw_payload"), schema)) \
                      .select("parsed_data.*", "ingested_at_hub")
    
    # Regra de Negócio: Criar um alerta se o status for critical ou leitura acima de 100
    df_silver = df_parsed.withColumn(
        "is_anomaly",
        when((col("status") == "critical") | (col("reading_value") > 100.0), True).otherwise(False)
    )
    
    return df_silver.select("timestamp", "device_id", "sensor_type", "reading_value", "is_anomaly")