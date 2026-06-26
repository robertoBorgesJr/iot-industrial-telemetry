import os
import json
import time
import random
import datetime
from azure.eventhub import EventHubProducerClient, EventData

CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = "telemetry"

def generate_iot_data():
    """Simula dados de sensores de telemetria de máquinas industriais."""
    sensor_types = ["temperature", "vibration", "pressure"]
    machine_id = f"MACH-{random.randint(100, 105)}"
    
    # Simulação de variações normais e picos esporádicos (anomalias)
    base_temp = 65.0 if machine_id != "MACH-103" else 85.0  # Máquina 103 está operando quente
    
    payload = {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "device_id": machine_id,
        "sensor_type": random.choice(sensor_types),
        "reading_value": round(base_temp + random.uniform(-5.0, 5.0), 2),
        "status": "normal"
    }
    
    # Injetando uma anomalia aleatória em 5% dos casos
    if random.random() < 0.05:
        payload["reading_value"] = round(payload["reading_value"] * 1.5, 2)
        payload["status"] = "critical"
        
    return payload

def send_to_eventhub():
    """Inicializa o produtor e envia eventos em streaming contínuo."""
    print("🚀 Inicializando Simulador de Sensores IoT...")
    
    # Cria o cliente produtor para o Event Hub
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR, 
        eventhub_name=EVENTHUB_NAME
    )
    
    try:
        with producer:
            print("📡 Streaming iniciado. Pressione Ctrl+C para parar.")
            while True:
                # 1. Cria um lote de eventos (batch) para otimizar o envio
                event_data_batch = producer.create_batch()
                
                # 2. Gera os dados do sensor
                data = generate_iot_data()
                json_data = json.dumps(data)
                
                # 3. Adiciona o evento ao lote
                event_data_batch.add(EventData(json_data))
                
                # 4. Envia o lote para o Azure Event Hubs
                producer.send_batch(event_data_batch)
                print(f"🔹 Evento enviado com sucesso: {json_data}")
                
                # Aguarda 1 segundo entre as leituras do sensor
                time.sleep(1.0)
                
    except KeyboardInterrupt:
        print("\n🛑 Simulador encerrado pelo usuário.")
    except Exception as e:
        print(f"\n❌ Erro crítico no simulador: {e}")

if __name__ == "__main__":
    send_to_eventhub()