import yaml
import os
from typing import Any, Dict

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Loads a YAML configuration file.

    Args:
        config_path (str): The absolute or relative path to the YAML configuration file.

    Returns:
        Dict[str, Any]: A dictionary representing the configuration.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        yaml.YAMLError: If there is an error parsing the YAML file.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        if config_data is None: # Handle empty YAML file case
            return {}
        return config_data
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file {config_path}: {e}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred while loading {config_path}: {e}")

if __name__ == '__main__':
    # This example assumes you have a 'config/sample_job_config.yaml' file
    # relative to the project root.
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sample_config_path = os.path.join(project_root, 'config', 'sample_job_config.yaml')

    # Create a dummy sample_job_config.yaml for testing if it doesn't exist
    # This part of the example in the prompt will create the actual sample_job_config.yaml
    # if it's run, but the task asks to create it separately.
    # For the purpose of this tool, this if __name__ == '__main__': block will only be part of the file content.
    # The actual creation of config/sample_job_config.yaml will be a separate step.
    if not os.path.exists(sample_config_path):
        os.makedirs(os.path.dirname(sample_config_path), exist_ok=True)
        with open(sample_config_path, 'w') as f:
            yaml.dump({
                'job_name': 'sample_etl',
                'input': {
                    'path': '/data/input/sample_data.csv',
                    'format': 'csv',
                    'schema': {
                        'col1': 'string',
                        'col2': 'integer'
                    }
                },
                'output': {
                    'path': '/data/output/processed_data.parquet',
                    'format': 'parquet'
                },
                'spark_settings': {
                    'spark.master': 'local[*]',
                    'spark.app.name': 'SampleETLJob'
                }
            }, f, default_flow_style=False)
        print(f"Created dummy config for testing: {sample_config_path}")

    try:
        print(f"Attempting to load config: {sample_config_path}")
        # This will try to load the sample_job_config.yaml which will be created in the next step.
        # If this script were run directly before the next step, it would use the dummy above.
        config = load_config(sample_config_path)
        print("Configuration loaded successfully:")
        print(yaml.dump(config, indent=2))

        print("\nTesting with a non-existent file:")
        try:
            load_config("config/non_existent_config.yaml")
        except FileNotFoundError as e:
            print(f"Correctly caught error: {e}")

        print("\nTesting with an invalid YAML file:")
        invalid_yaml_path = os.path.join(project_root, 'config', 'invalid_config.yaml')
        with open(invalid_yaml_path, 'w') as f:
            f.write("key: value: another_value") # Invalid YAML
        try:
            load_config(invalid_yaml_path)
        except yaml.YAMLError as e:
            print(f"Correctly caught error: {e}")
        finally:
            if os.path.exists(invalid_yaml_path): # Ensure cleanup only if file was created
                os.remove(invalid_yaml_path) # Clean up

    except Exception as e:
        print(f"An error occurred during the example usage: {e}")
