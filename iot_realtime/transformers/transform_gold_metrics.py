import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(df, *args, **kwargs):
    print("📈 Calculando métricas analíticas da Gold...")
    
    # Garantir que o event_time está no formato correto de data/hora
    df['event_time'] = pd.to_datetime(df['event_time'])
    
    # Criar uma coluna truncada por minuto para fazer o agrupamento (Janela de 1m)
    df['window_start'] = df['event_time'].dt.floor('1min')
    
    # Agrupamento analítico adaptado para o seu novo formato chave-valor
    df_gold = df.groupby(['window_start', 'device_id']).agg(
        val_medio=('temperature', 'mean'),
        val_maximo=('temperature', 'max'),
        val_minimo=('temperature', 'min'),
        total_eventos=('device_id', 'count')
    ).reset_index()
    
    return df_gold