terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# 1. Grupo de Recursos (Container lógico)
resource "azurerm_resource_group" "rg" {
  name     = "rg-${var.project_name}-prod"
  location = var.location
}

# 2. Data Lake Storage Gen2 (Standard LRS para economizar)
resource "azurerm_storage_account" "datalake" {
  name                     = lower(replace("st${var.project_name}prod", "-", ""))
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Ativa o Hierarchical Namespace (obrigatório para ADLS Gen2)
}

# Contêiner principal do sistema de arquivos
resource "azurerm_storage_data_lake_gen2_filesystem" "filesystem" {
  name               = "datalake"
  storage_account_id = azurerm_storage_account.datalake.id
}

# Estrutura de pastas da Medallion Architecture
resource "azurerm_storage_data_lake_gen2_path" "bronze" {
  path               = "bronze"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.filesystem.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "silver" {
  path               = "silver"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.filesystem.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "gold" {
  path               = "gold"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.filesystem.name
  storage_account_id = azurerm_storage_account.datalake.id
  resource           = "directory"
}

# 3. Event Hubs (Mensageria - Camada Standard com 1 TU)
resource "azurerm_eventhub_namespace" "eh_namespace" {
  name                = "evhns-${var.project_name}-prod"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  capacity            = 1
}

# Tópico (Event Hub) para onde os sensores vão enviar dados
resource "azurerm_eventhub" "iot_hub" {
  name                = "telemetry"
  namespace_name      = azurerm_eventhub_namespace.eh_namespace.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = 2
  message_retention   = 1 # Mantém as mensagens por 1 dia (mínimo na camada Standard)
}