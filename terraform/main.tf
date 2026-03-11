resource "google_storage_bucket" "raw_lake" {
  name          = "crypto-raw-data-lake"
  location      = var.location
  force_destroy = true
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "prod_lake" {
  name          = "crypto-analytics-prod"
  location      = var.location
  force_destroy = true
  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "binance_dataset" {
  dataset_id = "binance_data"
  location   = var.location
  description = "Raw data from Binance, CoinGecko and Fear&Greed API"
}

resource "google_bigquery_dataset" "dbt_dataset" {
  dataset_id = "dbt_analytics"
  location   = var.location
  description = "Transformed crypto market models managed by dbt"
}