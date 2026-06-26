output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "eventhub_primary_connection_string" {
  value     = azurerm_eventhub_namespace.eh_namespace.default_primary_connection_string
  sensitive = true
}