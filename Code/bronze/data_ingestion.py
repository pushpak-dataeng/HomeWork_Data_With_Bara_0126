
#Data ingestion from source system 
import sys
# Add config folder to Python path so we can import ingestion settings
sys.path.append("/Volumes/dev_project/bronze/source_system/config")

# Import ingestion configuration
from ingestion_config import INGESTION_CONFIG
# Loop through each ingestion configuration
for item in INGESTION_CONFIG:

      # Print which source is being ingested and target table name

    print(f"Ingesting {item['source']} -> dev_project.bronze.{item['table']}")

    # Read CSV file from the source path

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(item["path"])  # source path
    )

    # Write data to Bronze Delta table

    df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable(f"dev_project.bronze.{item['table']}")  # destination table/path
