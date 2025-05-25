from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pyspark.sql import SparkSession, DataFrame

# Import utilities (assuming they will be used by connectors)
# from .logging_utils import get_logger # Example
# from .config_utils import load_config # Example

# logger = get_logger(__name__) # Initialize logger if needed at module level

class BaseConnector(ABC):
    """
    Abstract base class for data connectors.
    All specific connectors should inherit from this class and implement
    the read and write methods.
    """
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the connector.

        Args:
            spark (SparkSession): The SparkSession instance.
            config (Dict[str, Any]): Configuration dictionary for the connector.
                                     This might include path, format, options, etc.
        """
        self.spark = spark
        self.config = config
        # self.logger = get_logger(self.__class__.__name__) # Connector-specific logger

    @abstractmethod
    def read(self) -> Optional[DataFrame]:
        """
        Reads data from the source.

        Returns:
            Optional[DataFrame]: Spark DataFrame containing the data, or None if read fails.
        """
        pass

    @abstractmethod
    def write(self, df: DataFrame) -> bool:
        """
        Writes data to the target.

        Args:
            df (DataFrame): Spark DataFrame to write.

        Returns:
            bool: True if write is successful, False otherwise.
        """
        pass

class FileConnector(BaseConnector):
    """
    A generic connector for reading from and writing to file systems
    supported by Spark (e.g., HDFS, local FS, GCS, S3).
    """
    def __init__(self, spark: SparkSession, file_config: Dict[str, Any]):
        """
        Initializes the FileConnector.

        Args:
            spark (SparkSession): The SparkSession instance.
            file_config (Dict[str, Any]): Configuration specific to this file operation.
                Expected keys:
                - 'path' (str): Path to the file or directory.
                - 'format' (str): Spark file format (e.g., 'csv', 'parquet', 'json', 'orc').
                - 'options' (Dict[str, Any], optional): Options for Spark's DataFrameReader/Writer.
                - 'mode' (str, optional): Write mode (e.g., 'overwrite', 'append'). Defaults to 'errorifexists'.
                - 'schema' (StructType or str, optional): Schema for reading.
                - 'partition_by' (list[str], optional): Columns to partition by when writing.
        """
        super().__init__(spark, file_config)
        self.path = file_config.get('path')
        self.format = file_config.get('format')
        self.options = file_config.get('options', {})
        self.mode = file_config.get('mode', 'errorifexists') # Default Spark write mode
        self.schema = file_config.get('schema') # Can be a DDL string or StructType
        self.partition_by = file_config.get('partition_by')

        if not self.path or not self.format:
            # self.logger.error("FileConnector requires 'path' and 'format' in config.")
            raise ValueError("FileConnector requires 'path' and 'format' in configuration.")

    def read(self) -> Optional[DataFrame]:
        """
        Reads data from the specified file path and format.
        """
        # self.logger.info(f"Reading data from path: {self.path}, format: {self.format}")
        print(f"FileConnector: Reading from {self.path} (Format: {self.format}) with options {self.options}")
        try:
            reader = self.spark.read.format(self.format)
            if self.schema:
                reader = reader.schema(self.schema)
            if self.options:
                reader = reader.options(**self.options)
            
            df = reader.load(self.path)
            # self.logger.info(f"Successfully read data from {self.path}")
            return df
        except Exception as e:
            # self.logger.error(f"Error reading data from {self.path}: {e}", exc_info=True)
            print(f"FileConnector: Error reading from {self.path}: {e}")
            return None

    def write(self, df: DataFrame) -> bool:
        """
        Writes DataFrame to the specified file path and format.
        """
        # self.logger.info(f"Writing data to path: {self.path}, format: {self.format}, mode: {self.mode}")
        print(f"FileConnector: Writing to {self.path} (Format: {self.format}, Mode: {self.mode})")
        try:
            writer = df.write.format(self.format).mode(self.mode)
            if self.options: # Options can also apply to writers (e.g., compression for parquet)
                writer = writer.options(**self.options)
            if self.partition_by:
                writer = writer.partitionBy(*self.partition_by)
            
            writer.save(self.path)
            # self.logger.info(f"Successfully wrote data to {self.path}")
            return True
        except Exception as e:
            # self.logger.error(f"Error writing data to {self.path}: {e}", exc_info=True)
            print(f"FileConnector: Error writing to {self.path}: {e}")
            return False

# Example of how other connectors might be structured (not implemented yet)
# class DatabaseConnector(BaseConnector):
#     def __init__(self, spark: SparkSession, db_config: Dict[str, Any]):
#         super().__init__(spark, db_config)
#         # Initialize DB connection parameters from db_config
#         # Use credential_utils.get_secret for passwords
#         pass

#     def read(self) -> Optional[DataFrame]:
#         # Implement database read logic using Spark JDBC
#         pass

#     def write(self, df: DataFrame) -> bool:
#         # Implement database write logic using Spark JDBC
#         pass

# class PubSubConnector(BaseConnector): # Or KafkaConnector, etc.
#     def __init__(self, spark: SparkSession, stream_config: Dict[str, Any]):
#         super().__init__(spark, stream_config)
#         # Initialize streaming connection parameters
#         pass

#     def read(self) -> Optional[DataFrame]: # Might return a streaming DataFrame
#         # Implement streaming read
#         pass

#     def write(self, df: DataFrame) -> bool: # Might write to a streaming sink
#         # Implement streaming write
#         pass


if __name__ == '__main__':
    # This is a basic example. For real use, SparkSession needs to be properly initialized.
    # You would typically get SparkSession from your PySpark application.
    
    # Create a dummy SparkSession for local testing
    try:
        spark_session = SparkSession.builder.appName("ConnectorsTest") \
            .master("local[*]") \
            .getOrCreate()

        # --- Test FileConnector ---
        print("\n--- Testing FileConnector ---")
        
        # Create dummy data and write it
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        columns = ["name", "id"]
        test_df = spark_session.createDataFrame(data, columns)
        
        temp_dir = "temp_connector_test_data" # Relative to project root
        import shutil, os
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # shared_utils -> project_root
        temp_output_path_csv = os.path.join(project_root, temp_dir, "test_output.csv")
        temp_output_path_parquet = os.path.join(project_root, temp_dir, "test_output.parquet")

        # Ensure temp_dir exists and is clean
        if os.path.exists(os.path.join(project_root, temp_dir)):
            shutil.rmtree(os.path.join(project_root, temp_dir))
        os.makedirs(os.path.join(project_root, temp_dir), exist_ok=True)

        print(f"Temp output CSV path: {temp_output_path_csv}")
        print(f"Temp output Parquet path: {temp_output_path_parquet}")

        csv_write_config = {
            'path': temp_output_path_csv,
            'format': 'csv',
            'options': {'header': 'true'},
            'mode': 'overwrite'
        }
        file_writer_csv = FileConnector(spark_session, csv_write_config)
        print("Attempting to write CSV...")
        if file_writer_csv.write(test_df):
            print("CSV write successful.")
            
            # Test reading the CSV
            csv_read_config = {
                'path': temp_output_path_csv,
                'format': 'csv',
                'options': {'header': 'true', 'inferSchema': 'true'}
            }
            file_reader_csv = FileConnector(spark_session, csv_read_config)
            print("Attempting to read CSV...")
            read_df_csv = file_reader_csv.read()
            if read_df_csv:
                print("CSV read successful. Data:")
                read_df_csv.show()
        else:
            print("CSV write failed.")

        parquet_write_config = {
            'path': temp_output_path_parquet,
            'format': 'parquet',
            'mode': 'overwrite'
        }
        file_writer_parquet = FileConnector(spark_session, parquet_write_config)
        print("\nAttempting to write Parquet...")
        if file_writer_parquet.write(test_df):
            print("Parquet write successful.")

            # Test reading the Parquet
            parquet_read_config = {
                'path': temp_output_path_parquet,
                'format': 'parquet'
            }
            file_reader_parquet = FileConnector(spark_session, parquet_read_config)
            print("Attempting to read Parquet...")
            read_df_parquet = file_reader_parquet.read()
            if read_df_parquet:
                print("Parquet read successful. Data:")
                read_df_parquet.show()
        else:
            print("Parquet write failed.")

        # Clean up temp directory
        # shutil.rmtree(os.path.join(project_root, temp_dir))
        # print(f"Cleaned up temp directory: {os.path.join(project_root, temp_dir)}")

    except Exception as e:
        print(f"An error occurred during connector example: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'spark_session' in locals():
            spark_session.stop()
        print("Spark session stopped.")
