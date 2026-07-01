import os
from pathlib import Path
from dotenv import load_dotenv
import duckdb

# 1. Carrega as credenciais do .env
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env")

STORAGE_ACCOUNT_NAME = "stiotanalyticsrbgsprod"
STORAGE_ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_ACCESS_KEY")

print("🦆 Inicializando o motor DuckDB Serverless...")
con = duckdb.connect(database=":memory:")

# 2. Instala e carrega as extensões necessárias
con.execute("INSTALL azure;")
con.execute("LOAD azure;")
con.execute("INSTALL delta;")
con.execute("LOAD delta;")

# 3. CRIAÇÃO DO SECRET (Injeta a credencial direto no Delta Kernel)
print("🔑 Configurando segredos de autenticação para o Delta Kernel...")
con.execute(f"""
    CREATE OR REPLACE SECRET azure_secret (
        TYPE AZURE,
        PROVIDER CONFIG,
        ACCOUNT_NAME '{STORAGE_ACCOUNT_NAME}',
        CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net'
    );
""")

# 4. Ajuste do Protocolo da URL para a Gold
GOLD_PATH = f"az://datalake/gold/telemetry_agg_1m"

print("🔍 Executando Query SQL Analítica na Camada Gold...")

try:
    # 5. Consulta SQL adaptada para o esquema chave-valor de Big Data
    query = f"""
        SELECT 
            device_id,
            window_start,
            sensor_type,
            ROUND(val_medio, 2) AS valor_medio,
            ROUND(val_maximo, 2) AS valor_maximo,
            total_eventos
        FROM delta_scan('{GOLD_PATH}')
        WHERE sensor_type = 'temperature' 
          AND val_medio > 60.0
        ORDER BY window_start DESC
        LIMIT 10;
    """
    
    df_result = con.execute(query).df()
    
    print("\n📊 --- RELATÓRIO ANALÍTICO (SQL SERVERLESS VIA DUCKDB) ---")
    if not df_result.empty:
        print(df_result.to_string(index=False))
    else:
        print("ℹ️ Nenhuma anomalia de temperatura acima de 60.0°C encontrada nos últimos registros.")
    
except Exception as e:
    print(f"\n❌ Erro ao processar a query SQL: {e}")