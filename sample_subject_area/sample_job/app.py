import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from typing import Dict, Any

# Assuming shared_utils is in PYTHONPATH or installed
from shared_utils import get_logger, load_config, FileConnector, MetadataManager

# Initialize logger for this job
logger = get_logger(__name__)

def get_spark_session(app_name: str = "SampleSparkJob", spark_config: Dict[str, Any] = None) -> SparkSession:
    """Initializes and returns a SparkSession."""
    logger.info(f"Initializing SparkSession for {app_name}...")
    builder = SparkSession.builder.appName(app_name)

    if spark_config:
        for key, value in spark_config.items():
            builder = builder.config(key, value)
    
    # For local testing, ensure master is set if not in spark_config
    if not spark_config or not any(k.lower() == 'spark.master' for k in spark_config):
         builder = builder.master("local[*]") # Default to local if not specified

    spark = builder.getOrCreate()
    logger.info("SparkSession initialized successfully.")
    # Log some basic Spark configurations
    # conf_list = spark.sparkContext.getConf().getAll()
    # for conf_item in conf_list:
    #     if "spark.master" in conf_item[0] or "spark.app.name" in conf_item[0]:
    #          logger.debug(f"Spark Conf: {conf_item[0]} = {conf_item[1]}")
    return spark

def process_data(spark: SparkSession, input_connector: FileConnector, output_connector: FileConnector) -> bool:
    """
    Simple data processing: reads from input, transforms, writes to output.
    """
    logger.info("Starting data processing...")
    input_df = input_connector.read()

    if input_df is None:
        logger.error("Failed to read input data. Aborting processing.")
        return False

    logger.info("Input data schema:")
    input_df.printSchema()
    logger.info("Sample input data:")
    input_df.show(5)

    # Simple transformation: Convert 'name' column to uppercase
    # This assumes a 'name' column exists from the sample_metadata.yaml or sample_job_config.yaml
    if 'name' in input_df.columns:
        transformed_df = input_df.withColumn("name_upper", upper(col("name")))
        logger.info("Applied transformation: Converted 'name' to uppercase as 'name_upper'.")
    else:
        logger.warning("Column 'name' not found in input_df. Skipping transformation.")
        transformed_df = input_df # Pass through if no 'name' column

    logger.info("Transformed data schema:")
    transformed_df.printSchema()
    logger.info("Sample transformed data:")
    transformed_df.show(5)

    logger.info("Writing processed data...")
    success = output_connector.write(transformed_df)
    if success:
        logger.info("Data processing completed and written successfully.")
    else:
        logger.error("Failed to write processed data.")
    return success

def main():
    """Main execution function for the PySpark job."""
    logger.info("Starting Sample PySpark Job")

    # Construct paths relative to the project root
    # __file__ is .../sample_subject_area/sample_job/app.py
    # project_root is three levels up.
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Load main job configuration
    # This demonstrates loading the job-specific config.
    # In a real scenario, a wrapper script might pass the config path or name.
    config_file_path = os.path.join(project_root, 'config', 'sample_job_config.yaml')
    logger.info(f"Loading job configuration from: {config_file_path}")
    try:
        job_config = load_config(config_file_path)
    except Exception as e:
        logger.error(f"Failed to load job configuration: {e}", exc_info=True)
        return

    # Load metadata (optional, but good practice)
    metadata_file_path = os.path.join(project_root, 'config', 'sample_metadata.yaml')
    logger.info(f"Loading metadata from: {metadata_file_path}")
    try:
        metadata_manager = MetadataManager(metadata_file_path)
        # Example: Get schema for a dataset defined in metadata
        # This assumes 'source_data' in job_config corresponds to a dataset_id in metadata
        # For this example, we directly use config values.
        # customer_schema_ddl = metadata_manager.get_schema(job_config.get('source_data', {}).get('dataset_id', 'customer_data_v1'))
        # logger.info(f"Schema for 'customer_data_v1' from metadata: {customer_schema_ddl}")
    except Exception as e:
        logger.warning(f"Could not load metadata: {e}", exc_info=True)
        # Proceed without metadata if it's optional for this job's logic

    # Get Spark session using settings from config if available
    spark_env = job_config.get('spark_environment', 'on_premise') # Default to on_premise
    spark_settings = job_config.get('spark_config', {}).get(spark_env, {})
    spark = get_spark_session(app_name=job_config.get('job_name', 'SampleApp'), spark_config=spark_settings)

    # Prepare source and destination connector configurations from job_config
    # These paths should be absolute or resolvable by Spark.
    # For local testing, they could be relative to where spark-submit is run or absolute.
    # The Dockerfile copies data, so paths inside the container will be relative to /app.
    
    # For this example, we'll create some dummy input data based on metadata/config
    # In a real job, this data would pre-exist.
    
    # --- Create Dummy Input Data ---
    # This is just for making the sample app runnable standalone.
    # Normally, data exists at the source_data.path.
    source_config = job_config.get('source_data', {})
    dummy_input_path_in_container = os.path.join("/app", source_config.get('path', 'data/input/sample_job_input_data/'))
    
    # Ensure the schema matches what process_data expects (e.g., has a 'name' column)
    # Using schema from sample_job_config.yaml: id, name, value, event_date
    dummy_data = [
        (1, "ProductA", 10.5, "2023-01-15"),
        (2, "ProductB", 20.0, "2023-01-16"),
        (3, "ProductC", 5.75, "2023-01-17")
    ]
    dummy_columns = ["id", "name", "value", "event_date"] # from sample_job_config.yaml schema_definition
    
    try:
        # Ensure the directory for dummy_input_path_in_container exists
        # The path in sample_job_config.yaml is "data/input/source_files/"
        # So dummy_input_path_in_container will be /app/data/input/source_files/
        os.makedirs(dummy_input_path_in_container, exist_ok=True) # Adjusted to use the full path
        temp_df = spark.createDataFrame(dummy_data, dummy_columns)
        temp_df.write.format(source_config.get('format', 'csv')).options(**source_config.get('options', {})).mode("overwrite").save(dummy_input_path_in_container)
        logger.info(f"Created dummy input data at: {dummy_input_path_in_container}")
    except Exception as e:
        logger.error(f"Could not create dummy input data: {e}", exc_info=True)
        spark.stop()
        return
            
    source_connector_config = {
        'path': dummy_input_path_in_container, # Use path inside container
        'format': source_config.get('format', 'csv'),
        'options': source_config.get('options', {}),
        # 'schema': source_config.get('schema_definition') # If schema is complex, pass StructType
    }
    
    destination_config = job_config.get('destination_data', {})
    destination_path_in_container = os.path.join("/app", destination_config.get('path', 'data/output/sample_job_output_data/'))
    # Ensure the directory for destination_path_in_container exists
    # The path in sample_job_config.yaml is "data/output/processed_data/"
    os.makedirs(destination_path_in_container, exist_ok=True) # Adjusted to use the full path

    destination_connector_config = {
        'path': destination_path_in_container, # Use path inside container
        'format': destination_config.get('format', 'parquet'),
        'mode': destination_config.get('mode', 'overwrite'),
        'partition_by': destination_config.get('partition_by')
    }

    input_connector = FileConnector(spark, source_connector_config)
    output_connector = FileConnector(spark, destination_connector_config)

    # Execute processing
    try:
        process_data(spark, input_connector, output_connector)
    except Exception as e:
        logger.error(f"An error occurred during data processing: {e}", exc_info=True)
    finally:
        logger.info("Stopping SparkSession.")
        spark.stop()
        logger.info("Sample PySpark Job finished.")

if __name__ == "__main__":
    main()
