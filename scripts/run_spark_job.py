import argparse
import yaml
import os
import sys
import subprocess
import shutil # Added for shutil.make_archive
from typing import Optional, List # Added for type hinting

# Ensure shared_utils can be imported if this script is run from project root
# (scripts/run_spark_job.py)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from shared_utils import load_config

def build_spark_submit_command(
    job_entry_point: str,
    job_config_path: str,
    env_config_path: str,
    environment: str,
    job_args: Optional[List[str]] = None # Corrected type hint from list to List
) -> List[str]: # Corrected type hint from list to List
    """
    Builds the spark-submit command list.

    Args:
        job_entry_point (str): Path to the main PySpark Python file (e.g., app.py).
        job_config_path (str): Path to the job's specific YAML configuration file.
        env_config_path (str): Path to the environments YAML configuration file.
        environment (str): The target environment (e.g., 'on_premise', 'gcp_dataproc', 'local_dev').
        job_args (Optional[list[str]]): Optional list of arguments to pass to the PySpark script.

    Returns:
        list[str]: A list of strings representing the spark-submit command and its arguments.
    """
    # Load environment configuration
    try:
        all_env_configs = load_config(env_config_path)
    except FileNotFoundError:
        print(f"Error: Environment configuration file not found at {env_config_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading environment configuration: {e}")
        sys.exit(1)

    env_conf = all_env_configs.get(environment)
    if not env_conf:
        print(f"Error: Environment '{environment}' not found in {env_config_path}")
        print(f"Available environments: {list(all_env_configs.keys())}")
        sys.exit(1)

    # Load job-specific configuration
    try:
        job_conf = load_config(job_config_path)
    except FileNotFoundError:
        print(f"Error: Job configuration file not found at {job_config_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading job configuration: {e}")
        sys.exit(1)

    # Start building the command
    if environment == "gcp_dataproc":
        print(f"Warning: For GCP DataProc, this script currently generates a spark-submit command.")
        print(f"Warning: A true GCP DataProc submission would use 'gcloud dataproc jobs submit pyspark ...'")
        print(f"Warning: And would need parameters like --cluster, --region, --py-files, etc.")
        pass # Proceed to build spark-submit for now


    command = ["spark-submit"]

    # Add master URL and deploy mode from environment config
    if env_conf.get('spark_master'):
        command.extend(["--master", env_conf['spark_master']])
    if env_conf.get('deploy_mode') and env_conf.get('spark_master') != 'local[*]':
        if not env_conf.get('spark_master','').startswith("k8s://"):
             command.extend(["--deploy-mode", env_conf['deploy_mode']])

    # Add environment-specific spark-submit options
    command.extend(env_conf.get('submit_options', []))

    job_spark_properties = {}
    if 'spark_config' in job_conf and environment in job_conf['spark_config']:
        job_spark_properties.update(job_conf['spark_config'][environment].get('spark_properties', {}))
    elif 'spark_settings' in job_conf: 
         job_spark_properties.update(job_conf.get('spark_settings',{}))

    merged_spark_properties = {**env_conf.get('spark_properties', {}), **job_spark_properties}

    for key, value in merged_spark_properties.items():
        command.extend(["--conf", f"{key}={value}"])
    
    # Prepare py_files list
    # Start with zipping shared_utils
    shared_utils_zip_base = os.path.join(PROJECT_ROOT, "shared_utils") # path to shared_utils dir
    shared_utils_zip_target = os.path.join(PROJECT_ROOT, "shared_utils.zip") # where to put shared_utils.zip
    
    # Ensure the target directory for the zip file exists (PROJECT_ROOT in this case)
    os.makedirs(os.path.dirname(shared_utils_zip_target), exist_ok=True)
    
    # Create the zip file
    shutil.make_archive(shared_utils_zip_base, 'zip', root_dir=PROJECT_ROOT, base_dir='shared_utils')
    
    effective_py_files = [shared_utils_zip_target] # Add the path to the created zip file

    # Add Python files from job config (e.g., other_python_scripts.py)
    job_dir = os.path.dirname(os.path.join(PROJECT_ROOT, job_entry_point)) # Make job_entry_point absolute before dirname
    for pf in job_conf.get('py_files', []):
        abs_pf_path = os.path.join(job_dir, pf)
        if os.path.exists(abs_pf_path):
             effective_py_files.append(abs_pf_path)
        else:
            print(f"Warning: PyFile not found, skipping: {abs_pf_path}")

    if effective_py_files: # Check if list is not empty
        command.extend(["--py-files", ",".join(effective_py_files)])

    # Jars (from job_config)
    jars = []
    for jar_path in job_conf.get('jars', []):
        abs_jar_path = os.path.join(PROJECT_ROOT, jar_path)
        if os.path.exists(abs_jar_path):
            jars.append(abs_jar_path)
        else:
            print(f"Warning: JAR file not found, skipping: {abs_jar_path}")
    if jars:
        command.extend(["--jars", ",".join(jars)])

    # Files (from job_config)
    files = []
    for file_path in job_conf.get('files', []):
        abs_file_path = os.path.join(PROJECT_ROOT, file_path)
        if os.path.exists(abs_file_path):
            files.append(abs_file_path)
        else:
            print(f"Warning: File not found, skipping: {abs_file_path}")
    if files:
        command.extend(["--files", ",".join(files)])
            
    # Main application Python file (relative to project root for spark-submit)
    command.append(job_entry_point)

    # Application arguments
    script_args_from_config = job_conf.get('application_arguments', [])
    final_script_args = job_args if job_args is not None else script_args_from_config
    
    if final_script_args:
        command.extend(final_script_args)
            
    return command

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build and optionally run a PySpark job submission command.")
    parser.add_argument("job_path", help="Path to the main PySpark application file relative to project root (e.g., sample_subject_area/sample_job/app.py).")
    parser.add_argument("environment", help="Execution environment (e.g., 'on_premise', 'gcp_dataproc', 'local_dev').")
    parser.add_argument("--job_config", help="Path to the job-specific YAML configuration file relative to project root. Defaults to a standard path if not provided (e.g., config/job_name_config.yaml).")
    parser.add_argument("--env_config", default="config/environments.yaml", help="Path to the environments YAML configuration file relative to project root.")
    parser.add_argument("--run", action="store_true", help="Actually execute the spark-submit command (use with caution).")
    parser.add_argument('script_args', nargs=argparse.REMAINDER, help="Arguments to pass to the PySpark script.")

    args = parser.parse_args()

    abs_job_entry_point = os.path.join(PROJECT_ROOT, args.job_path)
    if not os.path.exists(abs_job_entry_point):
        print(f"Error: Job entry point not found: {abs_job_entry_point}")
        sys.exit(1)

    job_config_file_path_arg = args.job_config
    if job_config_file_path_arg:
        job_config_file_path = os.path.join(PROJECT_ROOT, job_config_file_path_arg)
    else:
        job_name = os.path.splitext(os.path.basename(args.job_path))[0] 
        job_parent_dir_name = os.path.basename(os.path.dirname(args.job_path)) 
        
        default_config_name = f"{job_parent_dir_name}_config.yaml" 
        job_config_file_path = os.path.join(PROJECT_ROOT, "config", default_config_name)
        if not os.path.exists(job_config_file_path):
            default_config_name_fallback = f"{job_name}_config.yaml" 
            job_config_file_path_fallback = os.path.join(PROJECT_ROOT, "config", default_config_name_fallback)
            if os.path.exists(job_config_file_path_fallback):
                job_config_file_path = job_config_file_path_fallback
            else:
                print(f"Warning: Default job config {default_config_name} (or {default_config_name_fallback}) not found. Using sample_job_config.yaml as a fallback for demonstration.")
                job_config_file_path = os.path.join(PROJECT_ROOT, "config", "sample_job_config.yaml")

    if not os.path.exists(job_config_file_path):
         print(f"Error: Job config file could not be determined or found: {job_config_file_path}")
         sys.exit(1)
    
    abs_env_config_path = os.path.join(PROJECT_ROOT, args.env_config)
    if not os.path.exists(abs_env_config_path):
        print(f"Error: Environment config file not found: {abs_env_config_path}")
        sys.exit(1)

    print(f"--- Spark Job Submission Details ---")
    print(f"Project Root:    {PROJECT_ROOT}")
    print(f"Job Entry Point: {args.job_path}")
    print(f"Job Config:      {os.path.relpath(job_config_file_path, PROJECT_ROOT)}")
    print(f"Environment:     {args.environment}")
    print(f"Env Config:      {args.env_config}")
    if args.script_args:
         print(f"Script Arguments: {args.script_args}")
    print(f"------------------------------------")
    
    submit_command = build_spark_submit_command(
        args.job_path, # Relative path to job entry point
        job_config_file_path, # Absolute path to job config
        abs_env_config_path,  # Absolute path to env config
        args.environment,
        job_args=args.script_args
    )
    
    shared_utils_zip_path = os.path.join(PROJECT_ROOT, "shared_utils.zip")
    if os.path.exists(shared_utils_zip_path):
        os.remove(shared_utils_zip_path)
        # print(f"Cleaned up temporary {shared_utils_zip_path}") # Optional: for debugging

    print("\nGenerated spark-submit command:")
    print(" \\\n    ".join(submit_command)) # Print with line breaks for readability
    print("\n")

    if args.run:
        print("Executing command...")
        try:
            # Execute from project root
            process = subprocess.Popen(submit_command, cwd=PROJECT_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in iter(process.stdout.readline, b''): # type: ignore
                print(line.decode().strip())
            process.stdout.close() # type: ignore
            return_code = process.wait()
            if return_code == 0:
                print("Spark job submitted and executed successfully (or client returned success).")
            else:
                print(f"Spark job submission failed with return code: {return_code}")
        except FileNotFoundError:
            print("Error: spark-submit command not found. Is Spark installed and in PATH?")
        except Exception as e:
            print(f"Error executing spark-submit: {e}")
    else:
        print("Dry run: Command not executed. Use --run to execute.")
