# --- Configuration & Paths ---
# Define directory paths for easier maintenance
TERRAFORM_DIR = ./terraform
AIRFLOW_DIR   = ./airflow
DBT_DIR       = ./dbt_crypto

# .PHONY ensures that these targets are treated as commands, 
# not as files in the directory.
.PHONY: help setup infra-up docker-up dbt-run clean

# --- Default Target ---
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  setup      - Initialize Infrastructure and start Airflow"
	@echo "  infra-up   - Run Terraform init and apply"
	@echo "  docker-up  - Start Airflow containers via Docker Compose"
	@echo "  dbt-run    - Install dbt dependencies and run models"
	@echo "  clean      - Tear down containers and destroy GCP resources"

# --- Main Workflow ---

# Full project initialization: Provisions cloud resources and starts the environment
setup: infra-up docker-up

# Navigates to the terraform directory to provision GCS and BigQuery
infra-up:
	@echo ">>> Initializing and applying Terraform infrastructure..."
	cd $(TERRAFORM_DIR) && terraform init && terraform apply -auto-approve

# Starts the Airflow scheduler, webserver, and database in detached mode
docker-up:
	@echo ">>> Starting Airflow service containers..."
	cd $(AIRFLOW_DIR) && docker-compose up -d

# Installs required dbt packages and executes the transformation pipeline
dbt-run:
	@echo ">>> Running dbt transformations..."
	cd $(DBT_DIR) && dbt deps && dbt run

# --- Cleanup ---

# Stops Docker containers and destroys Terraform-managed infrastructure
# Warning: This will delete your BigQuery datasets and GCS buckets!
clean:
	@echo ">>> Shutting down Airflow..."
	cd $(AIRFLOW_DIR) && docker-compose down
	@echo ">>> Destroying cloud infrastructure..."
	cd $(TERRAFORM_DIR) && terraform destroy -auto-approve