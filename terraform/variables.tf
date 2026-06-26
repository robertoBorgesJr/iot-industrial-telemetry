variable "project_name" {
  type        = string
  default     = "iot-analytics-rbgs"
  description = "Nome base para os recursos do projeto"
}

variable "location" {
  type        = string
  default     = "eastus"
  description = "Região da Azure onde os recursos serão criados"
}