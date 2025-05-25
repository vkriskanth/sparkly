import unittest
import os
import shutil
from pyspark.sql import SparkSession

# Add project root to sys.path to allow importing shared_utils and the app
import sys
# Assuming test_app.py is in sample_subject_area/sample_job/tests/
# Project root is 4 levels up
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "shared_utils")) # Ensure shared_utils is found
sys.path.insert(0, os.path.join(project_root, "sample_subject_area", "sample_job")) # Ensure app can be imported

from sample_subject_area.sample_job.app import process_data, get_spark_session # Target app
from shared_utils import FileConnector # Utility for test setup

class TestSampleJobApp(unittest.TestCase):

    spark: SparkSession = None
    test_data_dir = "temp_test_data_sample_job" # Relative to project root

    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session(app_name="TestSampleJobApp")
        
        # Create test data directory relative to project root
        cls.abs_test_data_dir = os.path.join(project_root, cls.test_data_dir)
        if os.path.exists(cls.abs_test_data_dir):
            shutil.rmtree(cls.abs_test_data_dir)
        os.makedirs(cls.abs_test_data_dir)

        # Create dummy input data for testing process_data
        # This schema should match what process_data expects, particularly the 'name' column
        input_data_path = os.path.join(cls.abs_test_data_dir, "input_data")
        os.makedirs(input_data_path, exist_ok=True)
        
        dummy_data = [
            (1, "TestName1", 100.0, "2023-01-01"),
            (2, "AnotherName", 200.0, "2023-01-02")
        ]
        # Schema matches sample_job_config.yaml's source_data.schema_definition for consistency
        columns = ["id", "name", "value", "event_date"] 
        df = cls.spark.createDataFrame(dummy_data, columns)
        df.write.format("csv").option("header", "true").mode("overwrite").save(input_data_path)
        
        cls.input_path = input_data_path
        cls.output_path = os.path.join(cls.abs_test_data_dir, "output_data")
        # No need to create output_path dir, Spark will do it. If it exists, write might fail depending on mode.
        # For 'overwrite' mode, Spark handles existing directory by deleting it.
        if os.path.exists(cls.output_path): # Clean up if it exists from a previous failed run
            shutil.rmtree(cls.output_path)


    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            cls.spark.stop()
        # Clean up test data directory
        if os.path.exists(cls.abs_test_data_dir):
            shutil.rmtree(cls.abs_test_data_dir)

    def test_process_data(self):
        input_connector_config = {
            'path': self.input_path,
            'format': 'csv',
            'options': {'header': 'true', 'inferSchema': 'true'}
        }
        output_connector_config = {
            'path': self.output_path,
            'format': 'parquet', # Or any format
            'mode': 'overwrite'
        }

        input_connector = FileConnector(self.spark, input_connector_config)
        output_connector = FileConnector(self.spark, output_connector_config)

        success = process_data(self.spark, input_connector, output_connector)
        self.assertTrue(success, "process_data should return True on success")

        # Verify output (basic check: does output path contain files?)
        # A more thorough test would read the output and check contents.
        self.assertTrue(os.path.exists(self.output_path), f"Output path {self.output_path} should exist.")
        output_files = os.listdir(self.output_path)
        self.assertTrue(any(f.endswith(".parquet") for f in output_files), "Output Parquet files should exist")
        
        # Example of reading back and checking content
        output_df = self.spark.read.parquet(self.output_path)
        self.assertEqual(output_df.count(), 2)
        self.assertTrue("name_upper" in output_df.columns)
        
        # Collect and sort to ensure consistent row order for assertion
        # Note: This is a small dataset; for larger data, consider sampling or specific queries.
        results = output_df.orderBy("id").collect()
        self.assertEqual(results[0]["name_upper"], "TESTNAME1")
        self.assertEqual(results[1]["name_upper"], "ANOTHERNAME")


if __name__ == "__main__":
    # To run tests:
    # Ensure PySpark is in your PYTHONPATH or installed.
    # From the project root:
    # python -m unittest sample_subject_area/sample_job/tests/test_app.py
    unittest.main()
