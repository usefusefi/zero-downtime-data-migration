terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = { source = "hashicorp/aws" version = "~> 5.0" }
    google = { source = "hashicorp/google" version = "~> 4.0" }
    azurerm = { source = "hashicorp/azurerm" version = "~> 3.0" }
    kubernetes = { source = "hashicorp/kubernetes" version = "~> 2.0" }
  }
}

provider "aws" {
  region = var.aws_region
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

provider "azurerm" {
  features {}
}

variable "aws_region" {
  default = "us-east-1"
}

variable "gcp_project_id" {
  default = "your-gcp-project"
}

variable "gcp_region" {
  default = "us-central1"
}

resource "aws_s3_bucket" "migration_bucket" {
  bucket = "zero-downtime-migration-bucket"
  acl    = "private"

  versioning {
    enabled = true
  }

  tags = {
    Environment = "production"
    Purpose     = "zero-downtime-migration"
  }
}
