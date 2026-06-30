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

# 3. CRIAÇÃO DO SECRET (Isso injeta a credencial direto no Delta Kernel do Rust)
print("🔑 Configurando segredos de autenticação para o Delta Kernel...")
con.execute(f"""
    CREATE OR REPLACE SECRET azure_secret (
        TYPE AZURE,
        PROVIDER CONFIG,
        ACCOUNT_NAME '{STORAGE_ACCOUNT_NAME}',
        CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net'
    );
""")

# 4. Ajuste do Protocolo da URL (O Delta Kernel prefere 'az://' em vez de 'azure://')
GOLD_PATH = f"az://datalake/gold/telemetry_agg_1m"

print("🔍 Executando Query SQL na Camada Gold...")

try:
    # 5. Consulta SQL limpa usando o delta_scan nativo
    query = f"""
        SELECT 
            device_id,
            window_start,
            ROUND(avg_temperature, 2) AS temp_media,
            ROUND(avg_pressure, 2) AS pressao_media,
            total_events
        FROM delta_scan('{GOLD_PATH}')
        WHERE avg_temperature > 60.0
        ORDER BY window_start DESC
        LIMIT 10;
    """
    
    df_result = con.execute(query).df()
    
    print("\n📊 --- RELATÓRIO ANALÍTICO (SQL SERVERLESS VIA DUCKDB) ---")
    print(df_result.to_string(index=False))
    
except Exception as e:
    print(f"\n❌ Erro ao processar a query SQL: {e}")