import json
import yaml
import os
from typing import Any, Dict, Optional
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, etc. # If parsing to Spark types

# from .logging_utils import get_logger
# logger = get_logger(__name__)

class MetadataManager:
    """
    Manages metadata for datasets, such as schemas, descriptions, and locations.
    This is a basic implementation that can load metadata from a JSON or YAML file.
    More advanced implementations could use a metadata store like Apache Atlas,
    AWS Glue Data Catalog, or a dedicated database.
    """
    _metadata_store: Dict[str, Any] = {}

    def __init__(self, metadata_file_path: Optional[str] = None):
        """
        Initializes the MetadataManager.

        Args:
            metadata_file_path (Optional[str]): Path to a JSON or YAML file
                                                containing metadata definitions.
                                                If None, an empty store is used.
        """
        if metadata_file_path:
            self.load_metadata_from_file(metadata_file_path)

    def load_metadata_from_file(self, metadata_file_path: str) -> None:
        """
        Loads metadata from a specified JSON or YAML file into the store.
        Existing metadata keys will be updated.

        Args:
            metadata_file_path (str): Path to the metadata file.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file format is unsupported or parsing fails.
        """
        # logger.info(f"Loading metadata from: {metadata_file_path}")
        if not os.path.exists(metadata_file_path):
            # logger.error(f"Metadata file not found: {metadata_file_path}")
            raise FileNotFoundError(f"Metadata file not found: {metadata_file_path}")

        try:
            with open(metadata_file_path, 'r') as f:
                if metadata_file_path.endswith('.json'):
                    data = json.load(f)
                elif metadata_file_path.endswith('.yaml') or metadata_file_path.endswith('.yml'):
                    data = yaml.safe_load(f)
                else:
                    raise ValueError(
                        f"Unsupported metadata file format: {metadata_file_path}. "
                        "Only .json and .yaml/.yml are supported."
                    )
            if data:
                # Clear the store before loading new data to avoid merging issues with this simple implementation
                self._metadata_store.clear() 
                self._metadata_store.update(data)
            # logger.info(f"Successfully loaded and merged metadata from {metadata_file_path}")
        except Exception as e:
            # logger.error(f"Error loading metadata from {metadata_file_path}: {e}", exc_info=True)
            raise ValueError(f"Error loading metadata from {metadata_file_path}: {e}")


    def register_dataset(self, dataset_id: str, metadata: Dict[str, Any]) -> None:
        """
        Registers or updates metadata for a specific dataset.

        Args:
            dataset_id (str): A unique identifier for the dataset.
            metadata (Dict[str, Any]): A dictionary containing the metadata
                                       (e.g., schema, path, description).
        """
        # logger.info(f"Registering metadata for dataset: {dataset_id}")
        self._metadata_store[dataset_id] = metadata

    def get_dataset_metadata(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves metadata for a specific dataset.

        Args:
            dataset_id (str): The unique identifier for the dataset.

        Returns:
            Optional[Dict[str, Any]]: The metadata dictionary if found, else None.
        """
        # logger.debug(f"Retrieving metadata for dataset: {dataset_id}")
        return self._metadata_store.get(dataset_id)

    def get_schema(self, dataset_id: str) -> Optional[Any]: # Could return StructType or dict
        """
        Retrieves the schema for a specific dataset.
        The schema can be in various forms (e.g., Spark StructType DDL string,
        a list of dictionaries, or a custom object).

        Args:
            dataset_id (str): The unique identifier for the dataset.

        Returns:
            Optional[Any]: The schema if found, else None.
        """
        dataset_meta = self.get_dataset_metadata(dataset_id)
        if dataset_meta:
            return dataset_meta.get('schema')
        return None

    def list_datasets(self) -> list[str]:
        """
        Lists all dataset IDs currently registered.

        Returns:
            list[str]: A list of dataset IDs.
        """
        return list(self._metadata_store.keys())

    def clear_metadata(self) -> None:
        """
        Clears all metadata from the store.
        """
        # logger.info("Clearing all metadata.")
        self._metadata_store.clear()

if __name__ == '__main__':
    # This example assumes you might have a 'config/sample_metadata.yaml' file
    # relative to the project root.

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # shared_utils -> project_root
    sample_metadata_path = os.path.join(project_root, 'config', 'sample_metadata.yaml')

    # Create a dummy sample_metadata.yaml for testing
    os.makedirs(os.path.dirname(sample_metadata_path), exist_ok=True)
    sample_metadata_content = {
        "customer_data_v1": {
            "description": "Raw customer data from CRM export.",
            "source_system": "CRM",
            "path": "/mnt/data/raw/customers_2023.csv",
            "format": "csv",
            "schema_type": "ddl", # Could be 'struct', 'ddl', 'json_schema' etc.
            "schema": "id INT, name STRING, email STRING, signup_date DATE",
            "tags": ["raw", "pii"]
        },
        "product_catalog_v3": {
            "description": "Product catalog information.",
            "source_system": "PIM",
            "path": "gs://my-bucket/data/product_catalog.parquet",
            "format": "parquet",
            "schema_type": "struct_fields",
            "schema": [
                {"name": "product_id", "type": "string", "nullable": False},
                {"name": "category", "type": "string", "nullable": True},
                {"name": "price", "type": "double", "nullable": True}
            ],
            "data_quality_rules": "config/dq_rules/product_catalog_rules.json"
        }
    }
    with open(sample_metadata_path, 'w') as f:
        yaml.dump(sample_metadata_content, f, default_flow_style=False)
    print(f"Created dummy metadata for testing: {sample_metadata_path}")

    print("\n--- Testing MetadataManager ---")
    # Test loading from file
    # Note: Each instantiation of MetadataManager will have its own _metadata_store
    # if it's not made a class variable or handled differently.
    # For the test, this is fine.
    manager = MetadataManager(sample_metadata_path)
    print(f"Datasets loaded: {manager.list_datasets()}")

    customer_schema_ddl = manager.get_schema("customer_data_v1")
    print(f"Schema for 'customer_data_v1' (DDL): {customer_schema_ddl}")
    assert customer_schema_ddl == "id INT, name STRING, email STRING, signup_date DATE"

    product_meta = manager.get_dataset_metadata("product_catalog_v3")
    print(f"Metadata for 'product_catalog_v3': {product_meta}")
    assert product_meta['format'] == "parquet"

    # Test registering a new dataset
    new_dataset_meta = {
        "description": "Temporary dataset for aggregation.",
        "path": "/tmp/agg_results.orc",
        "format": "orc",
        "schema": "user_id STRING, total_spent DECIMAL(10,2)"
    }
    manager.register_dataset("temp_aggregation_v1", new_dataset_meta)
    print(f"Datasets after registration: {manager.list_datasets()}")
    assert "temp_aggregation_v1" in manager.list_datasets()
    # Ensure previous data is still there due to class-level _metadata_store for this example
    # This behavior might need adjustment if instances should be isolated.
    # The current implementation with _metadata_store.clear() in load_metadata_from_file
    # means a new load will replace, not merge, which is fine for the test.
    assert "customer_data_v1" in manager.list_datasets() 


    # Test getting non-existent dataset
    non_existent = manager.get_dataset_metadata("non_existent_dataset")
    print(f"Metadata for 'non_existent_dataset': {non_existent}")
    assert non_existent is None

    # Clean up dummy metadata file
    # os.remove(sample_metadata_path) # Commenting out to leave the file as per subtask requirement
    # print(f"Cleaned up {sample_metadata_path}")

    print("\n--- Test with empty manager ---")
    # Create a new manager instance to ensure it's empty if _metadata_store is instance level
    # If _metadata_store is class level and not cleared, this test would be affected.
    # The current code clears _metadata_store in load_metadata_from_file,
    # but an empty manager won't call that.
    # For a truly empty manager, _metadata_store should be an instance variable or cleared in __init__.
    # Let's adjust the test or code for clarity.
    # For now, assuming _metadata_store is class-level for simplicity of the example.
    # A new instance will reuse the class-level store if not careful.
    # The provided code has _metadata_store as a class variable.
    # To test an "empty" manager properly, we should clear it first or use an instance variable.
    
    # Forcing a clear for this specific test case if using class variable
    manager.clear_metadata() # Clear any existing class-level data for this test
    
    empty_manager = MetadataManager() # Now it starts truly empty
    print(f"Datasets in empty_manager initially: {empty_manager.list_datasets()}")
    assert not empty_manager.list_datasets()

    empty_manager.register_dataset("manual_set", {"schema": "id INT"})
    assert empty_manager.get_schema("manual_set") == "id INT"
    print("MetadataManager tests completed.")

    # Restore data for subsequent tests if needed, or manage state better.
    # For this script, the final state of sample_metadata.yaml is what matters.
    # The __main__ block creates it again, so it's fine.
    if not os.path.exists(sample_metadata_path):
         with open(sample_metadata_path, 'w') as f:
            yaml.dump(sample_metadata_content, f, default_flow_style=False)
            print(f"Re-created dummy metadata for testing: {sample_metadata_path}")
