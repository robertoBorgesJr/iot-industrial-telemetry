import os
import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data(*args, **kwargs):
    # O Mage consegue ler as variáveis do arquivo .env automaticamente se estiver na raiz
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "stiotanalyticsrbgsprod")
    account_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")
    
    silver_path = "az://datalake/silver/telemetry/"
    
    print("📖 Mage carregando dados da camada Silver na Azure...")
    df_silver = pd.read_parquet(
        silver_path,
        storage_options={"account_name": account_name, "account_key": account_key}
    )
    return df_silver