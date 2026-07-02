# 🛠️ IoT Real-Time Analytics Pipeline

Pipeline de dados ponta a ponta para ingestão, transformação, monitoramento e análise analítica de telemetria de sensores IoT industriais em tempo real, utilizando a **Arquitetura Medalhão** sobre um Data Lakehouse na nuvem.

## 📐 Arquitetura do Projeto

O ecossistema foi projetado para processar fluxos contínuos de dados sob forte consistência transacional (ACID) e monitoramento de anomalias:

1. **Simulador IoT:** Geração contínua de payloads JSON simulando sensores industriais.
2. **Camada Bronze (Delta Lake):** Ingestão do fluxo bruto persistindo metadados de auditoria.
3. **Camada Silver (Delta Lake):** Tratamento, decodificação dos payloads analíticos, enriquecimento e aplicação de regras de negócio (classificação de anomalias com alerta real-time via Webhook do Discord).
4. **Camada Gold (Delta Lake):** Agregador temporal estruturado em janelas deslizantes (Windowing & Watermarking) de 1 minuto agrupando por métricas chaves-valor de sensores.
5. **Orquestração (Mage.ai):** Controle de dependências e gatilhos automatizados locais.
6. **Consumo de Dados:** Queries analíticas Serverless de alta performance usando **DuckDB**.

---

## 📋 Pré-requisitos

Antes de iniciar, certifique-se de ter instalado em sua máquina local:
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Anaconda / Miniconda](https://docs.anaconda.com/anaconda/install/) (Python 3.10+)
- Conta na **Azure** com um recurso de *Storage Account Gen2* (Hierarchical Namespace habilitado).

---

## 🔐 Configuração do Ambiente Local (.env)

Crie um arquivo chamado `.env` na raiz do projeto com a seguinte estrutura de credenciais:

```env
# Azure Storage Account Config
AZURE_STORAGE_ACCESS_KEY=sua_chave_de_acesso_aqui

# Sistema de Alertas (Webhook)
WEBHOOK_ALERTS_URL=[https://discord.com/api/webhooks/suu_url_aqui](https://discord.com/api/webhooks/suu_url_aqui)

# Configurações Internas do Mage
MAGE_REQUIRE_AUTHENTICATION=0

🧙‍♂️ Como Executar o Orquestrador (Mage.ai)

O projeto utiliza o Mage.ai rodando em container Docker mapeando o código fonte local. Para inicializar o painel de desenvolvimento desativando telas de autenticação/login rígidas, execute o comando correspondente ao seu terminal:

No Windows (Command Prompt / CMD)
docker run -it --rm -p 6789:6789 -e MAGE_REQUIRE_AUTHENTICATION=0 -v "%cd%:/home/src" mageai/mageai mage start iot_orchestrator

No Linux / macOS / PowerShell / Git Bash
docker run -it --rm -p 6789:6789 -e MAGE_REQUIRE_AUTHENTICATION=0 -v "$(pwd):/home/src" mageai/mageai mage start iot_orchestrator


💡 Após a inicialização, abra o painel analítico em: http://localhost:6789

🚀 Execução do Pipeline de Streaming (Apache Spark)
Devido às otimizações de rede necessárias no ambiente de desenvolvimento local (Windows), a ordem cronológica de inicialização dos scripts no terminal é mandatória:
1. Inicializar o Simulador: Garanta que mensagens estejam caindo no Hub/Bronze.
2. Executar a Camada Silver:
python src/streaming/stream_silver.py

Aguarde o processamento bem-sucedido do primeiro Micro-Batch (Batch ID 0) para inicializar a estrutura transacional Delta.

3. Executar a Camada Gold:
python src/streaming/stream_gold.py


📊 Consulta Serverless na Gold (DuckDB)
Para validar ou auditar os agregados consolidados diretamente na Azure sem a necessidade de subir um cluster Spark, execute o motor serverless:
python src/analytics/query_gold_sql.py

🤖 Integração e Entrega Contínua (CI/CD) via GitHub Actions
O repositório possui fluxos de trabalho automatizados para validação do código via Pull Requests. Para que o pipeline consiga se autenticar de forma segura e federada com os recursos da Azure (evitando senhas estáticas expostas), é utilizada a autenticação nativa baseada em OIDC (OpenID Connect).
É obrigatório cadastrar as seguintes variáveis secretas em seu repositório (Settings > Secrets and variables > Actions):

Secret Name                 Descrição
AZURE_CLIENT_ID             ID do aplicativo (Application/Client ID) registrado no Azure Entra ID com permissões de Contribuidor de Dados.
AZURE_TENANT_ID             ID do diretório (Tenant ID) da sua subscrição Azure.
AZURE_SUBSCRIPTION_ID       ID da subscrição da Azure (Subscription ID) ativa onde o cluster/storage reside.AZURE_STORAGE_ACCESS_KEY    Chave de acesso usada nas baterias de validações automatizadas.
WEBHOOK_ALERTS_URL          Endpoint do canal do Discord para testes de alertas integrados.

🛠️ Configurações de Notificação de Pipeline
Em caso de quebra ou falhas em Pull Requests de ramos de desenvolvimento (feat/*), o pipeline disparará alertas automáticos por e-mail para os proprietários do repositório. Para interromper execuções pendentes ou silenciar alertas repetitivos durante refatorações longas, utilize o botão "Close pull request" no GitHub ou converta o PR para o status de "Draft" (Rascunho) na barra lateral direita do repositório.
