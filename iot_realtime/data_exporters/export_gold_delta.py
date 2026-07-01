import os
import pandas as pd
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(df_gold, *args, **kwargs):
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "stiotanalyticsrbgsprod")
    account_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")
    
    gold_path = "az://datalake/gold/telemetry_agg_mage/"
    
    print(f"💾 Salvando {len(df_gold)} linhas agregadas na Gold via Mage...")
    df_gold.to_parquet(
        gold_path,
        index=False,
        storage_options={"account_name": account_name, "account_key": account_key}
    )