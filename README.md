# Data Engineering Repository

This repository is designed for developing, managing, and deploying PySpark jobs. It provides a structured framework for building scalable and maintainable data pipelines.

## Table of Contents

- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
  - [1. Clone the Repository](#1-clone-the-repository)
  - [2. Python Environment](#2-python-environment)
  - [3. Install Dependencies](#3-install-dependencies)
  - [4. Local Secrets (Optional)](#4-local-secrets-optional)
- [Shared Utilities](#shared-utilities)
  - [Configuration (`shared_utils.config_utils`)](#configuration-shared_utilsconfig_utils)
  - [Logging (`shared_utils.logging_utils`)](#logging-shared_utilslogging_utils)
  - [Credentials (`shared_utils.credential_utils`)](#credentials-shared_utilscredential_utils)
  - [Connectors (`shared_utils.connectors`)](#connectors-shared_utilsconnectors)
  - [Metadata (`shared_utils.metadata_utils`)](#metadata-shared_utilsmetadata_utils)
- [Running Jobs](#running-jobs)
  - [1. Configure Your Job](#1-configure-your-job)
  - [2. Configure Environment](#2-configure-environment)
  - [3. Execute the Job](#3-execute-the-job)
  - [Example: Running the Sample Job](#example-running-the-sample-job)
- [Job Structure](#job-structure)
- [Dockerization](#dockerization)
- [Testing](#testing)
  - [Running Utility Tests](#running-utility-tests)
  - [Running Job-Specific Tests](#running-job-specific-tests)
- [Contributing](#contributing)
- [Future Enhancements](#future-enhancements)

## Overview

This framework aims to provide:
-   **Environment Agnostic Execution:** Run PySpark jobs seamlessly on local machines, on-premise Spark clusters, or GCP DataProc.
-   **Reusable Components:** Leverage shared utilities for common tasks like logging, configuration management, data connections, and metadata handling.
-   **Parameterized Configuration:** Externalize job and environment settings using YAML files for flexibility.
-   **Standardized Structure:** Organize jobs logically by subject area with a consistent folder layout.

## Repository Structure

-   `config/`: Contains YAML configuration files.
    -   `environments.yaml`: Defines Spark settings for different execution environments (local, on-prem, GCP).
    -   `sample_job_config.yaml`: Example configuration for a specific job.
    -   `sample_metadata.yaml`: Example metadata definitions.
-   `sample_subject_area/`: Example directory for a specific data domain.
    -   `sample_job/`: Contains a sample PySpark job.
        -   `app.py`: Main application script for the job.
        -   `requirements.txt`: Python dependencies for the job.
        -   `Dockerfile`: Docker definition for containerizing the job.
        -   `dag_folder/`: (Optional) For Airflow DAG definitions.
            -   `sample_job_dag.py`: An example Airflow DAG for the sample job.
        -   `tests/`: Unit/integration tests for this specific job.
-   `scripts/`: Utility scripts for the repository.
    -   `run_spark_job.py`: Script to construct and run `spark-submit` commands.
-   `shared_utils/`: Reusable Python modules (utilities).
    -   `__init__.py`
    -   `config_utils.py`
    -   `logging_utils.py`
    -   `credential_utils.py`
    -   `connectors.py`
    -   `metadata_utils.py`
-   `README.md`: This file.
-   `.gitignore`: Specifies intentionally untracked files.
-   `LICENSE`: Project license.

## Prerequisites

-   Python 3.8+
-   Apache Spark (installed and configured for your target environment, or use Docker)
    -   Ensure `spark-submit` is in your `PATH` if running locally or on-premise without Docker.
-   `pip` and `venv` (or your preferred Python package manager)

## Setup

### 1. Clone the Repository
```bash
git clone <repository_url>
cd <repository_name>
```

### 2. Python Environment
It's recommended to use a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
Install global dependencies (e.g., PyYAML for utilities, if not bundled or provided by Spark):
```bash
# There isn't a global requirements.txt yet, but shared_utils might need some.
# For now, PyYAML is used by shared_utils and Spark usually provides PySpark.
pip install pyyaml
# You might also need to install pyspark if you want to run tests or apps locally
# and it's not provided by your system Spark installation.
# pip install pyspark>=3.0.0
```
Job-specific dependencies are listed in their respective `requirements.txt` files.

### 4. Local Secrets (Optional)
For local development, `credential_utils.py` can read from a `.local_secrets.json` file in the project root. **Do not commit this file.**
Example `.local_secrets.json`:
```json
{
  "MY_API_KEY": "your_local_api_key",
  "DB_PASSWORD": "your_local_db_password"
}
```
Ensure `.local_secrets.json` is listed in your `.gitignore` file.

## Shared Utilities

### Configuration (`shared_utils.config_utils`)
-   `load_config(config_path: str) -> Dict`: Loads a YAML configuration file.

### Logging (`shared_utils.logging_utils`)
-   `get_logger(name: str, level: int = logging.INFO) -> logging.Logger`: Returns a standardized logger.

### Credentials (`shared_utils.credential_utils`)
-   `get_secret(secret_name: str) -> str | None`: Retrieves secrets. (Placeholder for actual vault/secrets integration).

### Connectors (`shared_utils.connectors`)
-   `BaseConnector`: Abstract base class for data connectors.
-   `FileConnector`: Concrete implementation for Spark's file-based sources/sinks (CSV, Parquet, JSON, ORC, etc.).

### Metadata (`shared_utils.metadata_utils`)
-   `MetadataManager`: Manages metadata (schemas, descriptions, etc.) loaded from files or registered programmatically.

## Running Jobs

The `scripts/run_spark_job.py` script is used to submit PySpark jobs.

### 1. Configure Your Job
Job configurations are defined in YAML files within the `config/` directory (e.g., `config/sample_job_config.yaml`). These files specify:
-   Input/output paths and formats.
-   Transformations and business logic parameters.
-   Spark settings specific to the job (which can override environment settings).
-   Dependent files (`py_files`, `jars`, `files`).

### 2. Configure Environment
The `config/environments.yaml` file defines Spark configurations for different execution environments:
-   `on_premise`: For on-premise Hadoop clusters.
-   `gcp_dataproc`: For GCP DataProc clusters.
-   `local_dev`: For local development and testing.
Specify `spark_master`, `deploy_mode`, and other `spark-submit` options here.

### 3. Execute the Job
Use `python scripts/run_spark_job.py <job_script_path> <environment_name> [options]`.

**Arguments for `run_spark_job.py`:**
-   `job_path`: Relative path to the main Python script for the job (e.g., `sample_subject_area/sample_job/app.py`).
-   `environment`: Name of the environment to use from `config/environments.yaml` (e.g., `local_dev`, `on_premise`).
-   `--job_config` (optional): Path to a specific job configuration YAML file. If not provided, it attempts to find a config named `config/<job_directory_name>_config.yaml` (e.g. `config/sample_job_config.yaml`).
-   `--env_config` (optional): Path to the environments configuration file (defaults to `config/environments.yaml`).
-   `--run` (optional): Actually execute the `spark-submit` command. Without this, it prints the command (dry run).
-   `script_args` (optional, at the end): Arguments to be passed to your PySpark application.

### Example: Running the Sample Job
This example runs the sample job in a local development environment. It assumes Spark is available locally.

1.  **Ensure `config/sample_job_config.yaml` and `config/environments.yaml` are set up.**
    The defaults provided should work for a basic local run. The `sample_job/app.py` creates its own dummy input data for demonstration.

2.  **Run the script (dry run first to see the command):**
    ```bash
    # Ensure you are in the project root directory
    # Make sure shared_utils can be imported (e.g. venv activated, PYTHONPATH set if needed)
    python scripts/run_spark_job.py sample_subject_area/sample_job/app.py local_dev
    ```

3.  **Execute the job:**
    ```bash
    python scripts/run_spark_job.py sample_subject_area/sample_job/app.py local_dev --run
    ```
    Output data for the sample job will be written to `/app/data/output/sample_job_output_data/` *inside the conceptual Docker container path*, or relative to your project root if paths in `sample_job_config.yaml` are adjusted for local non-Docker runs. The current `sample_job/app.py` creates dummy input in `/app/data/input/...` and writes output to `/app/data/output/...` which will translate to `./data/input/...` and `./data/output/...` relative to the project root if you run `app.py` directly or if `spark-submit` is run from project root with these paths.

### Orchestration with Airflow (Example)

The `sample_subject_area/sample_job/dag_folder/sample_job_dag.py` provides an example of how this job could be orchestrated using Apache Airflow.
The sample DAG uses a `BashOperator` to execute the `scripts/run_spark_job.py` script.

**Key considerations for Airflow integration:**
-   **DAG Placement:** Airflow needs access to the DAG file. This typically means placing it in Airflow's configured DAGs folder.
-   **Repository Access:** The Airflow worker environment must have access to the entire repository code, especially `scripts/run_spark_job.py`, `shared_utils/`, and the job's own files. This can be achieved by:
    -   Including the repository code in the Airflow worker Docker images.
    -   Using Airflow's GitSync feature.
    -   Manually placing the repository where workers can access it.
-   **Path Configuration:** The `repo_home_placeholder` variable within `sample_job_dag.py` must be updated to the actual path of the repository root in the Airflow worker environment. This can be managed via Airflow Variables or environment variables.
-   **Dependencies:** The Airflow worker environment will need Python, and potentially `apache-airflow-providers-apache-spark` if you choose to use the `SparkSubmitOperator`.

## Job Structure

Each job is organized within a subject area directory (e.g., `wc_warranty_claims/`, `sales/`). An individual job folder (e.g., `job_1/`) should typically contain:
-   `app.py`: Main PySpark application entry point.
-   `other_python_scripts.py`: Additional Python modules used by `app.py`. These should be included in `py_files` in the job config if needed.
-   `requirements.txt`: Job-specific Python dependencies.
-   `Dockerfile`: For containerizing the job, especially for cloud or consistent deployments.
-   `dag_folder/`: (Optional) For Airflow DAG definitions if orchestrating this job (e.g., `sample_job_dag.py`).
-   `tests/`: Unit and integration tests for the job's logic.
    -   `test_app.py`: Example test file.

## Dockerization

The provided `sample_subject_area/sample_job/Dockerfile` is a basic template. It demonstrates:
-   Copying shared utilities and job-specific code.
-   Installing dependencies.
-   Setting `PYTHONPATH`.

To build a Docker image for the sample job (from the project root):
```bash
docker build -t sample-spark-job -f sample_subject_area/sample_job/Dockerfile .
```

Running this Docker image usually requires a Spark environment that the job can submit to, or running Spark itself within Docker (more complex setup). The `CMD` in the Dockerfile is a placeholder. For DataProc, you'd typically build the image, push it to a registry (like GCR), and then reference it in your DataProc job submission.

## Testing

### Running Utility Tests
Some utilities in `shared_utils/` have `if __name__ == '__main__':` blocks that can be run directly for basic checks:
```bash
python shared_utils/config_utils.py
python shared_utils/logging_utils.py
# etc.
```
Proper unit tests for shared utilities would ideally be in a top-level `tests/shared/` directory (this is a future enhancement).

### Running Job-Specific Tests
Job-specific tests are located in the `tests/` subdirectory of each job (e.g., `sample_subject_area/sample_job/tests/`).
You can run these using `unittest` or `pytest`.

**Using unittest:**
```bash
# Ensure pyspark is available in your environment or installed via pip
# Ensure shared_utils and the job code are in PYTHONPATH
# From the project root:
python -m unittest sample_subject_area/sample_job/tests/test_app.py
```

**Using pytest (if installed and preferred):**
```bash
# From the project root:
pytest sample_subject_area/sample_job/tests/test_app.py
```

## CI/CD Workflow

This repository implements a CI/CD pipeline using GitHub Actions to manage the lifecycle of code changes from feature development through to production readiness. The strategy involves feature branches, a `develop` branch for integration and QA, and a `main` branch for production releases.

### Branching Strategy

1.  **Feature Branches:** Developers create feature branches (e.g., `feature/my-new-job`, `fix/bug-in-connector`) from the `develop` branch.
2.  **`develop` Branch:** Feature branches are merged into `develop` after successful CI checks (including conceptual dev deployment and tests). The `develop` branch is then deployed to a QA environment for further testing.
3.  **`main` Branch:** After successful QA, the `develop` branch is merged into `main`. The `main` branch represents production-ready code and is used for deployments to the Production environment.

### Workflows

#### 1. Feature Branch CI (`.github/workflows/feature_branch_ci.yaml`)

*   **Purpose:** Ensures changes in feature branches are built, tested, and ready for integration into the `develop` branch.
*   **Triggers:**
    *   Push to any branch **except** `main` or `develop`.
    *   Pull Request opened or updated targeting the `develop` branch.
*   **Key Steps:**
    1.  **Checkout Repository:** Fetches the code.
    2.  **Set up Python:** Initializes Python 3.9.
    3.  **Install Dependencies:** Installs global and job-specific Python packages.
    4.  **Lint Code (Placeholder):** Placeholder for future linting integration.
    5.  **Run Unit Tests:** Executes unit tests (e.g., for `sample_job`).
    6.  **Deploy to Development (Conceptual):** Simulates deployment to a dev environment by running the `sample_job` via `scripts/run_spark_job.py` with `local_dev` settings.
    7.  **Create Pull Request to `develop`:** If all steps succeed, automatically creates a Pull Request from the feature branch to `develop`.

#### 2. QA Workflow (`.github/workflows/qa_workflow.yaml`)

*   **Purpose:** Automates deployment to and testing in the QA environment upon changes to the `develop` branch, and prepares changes for production release.
*   **Triggers:**
    *   Push to the `develop` branch (typically after a feature branch PR is merged).
*   **Key Steps:**
    1.  **Deploy to QA Environment (Conceptual):**
        *   Checks out the `develop` branch.
        *   Sets up Python and installs dependencies.
        *   Simulates deployment to QA by running `scripts/run_spark_job.py sample_subject_area/sample_job/app.py qa --run`. (Requires `qa` environment in `config/environments.yaml`).
    2.  **Test in QA (Conceptual):**
        *   Runs after successful QA deployment.
        *   Simulates running tests (e.g., integration tests, E2E tests) against the QA deployment. Currently, it re-runs the sample job's unit tests as a placeholder.
    3.  **Create Pull Request to `main` for Production:**
        *   If QA deployment and testing succeed, automatically creates a Pull Request from the `develop` branch to the `main` branch.
        *   This PR is intended for review by a Release Manager before merging into `main` for a production release.

#### 3. Production Deployment Workflow (`.github/workflows/production_workflow.yaml`)

*   **Purpose:** Automates the deployment of production-ready code to the Production environment.
*   **Triggers:**
    *   Push of a tag matching the pattern `C*` (e.g., `C123456`) to the `main` branch. This tag is typically applied by a Release Manager and serves as the "Change Number".
*   **Key Steps:**
    1.  **Checkout Repository:** Fetches the code corresponding to the pushed tag on `main`.
    2.  **Extract Tag Name:** The tag (Change Number) is extracted for logging and potential use in deployment scripts.
    3.  **Set up Python:** Initializes Python 3.9.
    4.  **Install Dependencies:** Installs global and job-specific Python packages.
    5.  **Deploy to Production (Conceptual):**
        *   Simulates deployment to the Production environment.
        *   Uses `scripts/run_spark_job.py sample_subject_area/sample_job/app.py production --run`. (Requires `production` environment in `config/environments.yaml`).
        *   This step includes placeholders in the workflow for passing production secrets (e.g., `${{ secrets.PROD_DB_PASSWORD }}`) as environment variables. The application code (`app.py` or shared utilities) would need to be designed to consume these secrets.
    6.  **Post-Deployment Validation (Conceptual):**
        *   Simulates running post-deployment checks (e.g., smoke tests, health checks) against the production deployment. Currently, it re-runs sample unit tests as a placeholder.

**Considerations for all workflows:**
-   **Conceptual Steps:** "Deploy to Development," "Deploy to QA," "Test in QA," "Deploy to Production," and "Post-Deployment Validation" are currently conceptual and use the sample job with specific configurations (`local_dev`, `qa`, `production`). Real implementations would involve actual deployment scripts, target environment configurations, dedicated test suites, and robust secret management.
-   **Secrets Management:** For actual deployments to secured environments, robust secrets management (using GitHub Secrets for API keys, cluster credentials, etc.) is essential. The Production workflow includes commented-out examples of how secrets would be exposed as environment variables.
-   **Error Handling and Notifications:** Production-grade workflows would include more sophisticated error handling, notifications (e.g., on Slack or email for failures/successes), and potentially manual approval gates or rollback procedures.
-   **Linting:** The linting step in `feature_branch_ci.yaml` is a placeholder and should be implemented with a chosen linter.
-   **Change Number Usage:** The "Change Number" (tag) is captured in the Production workflow. It can be used for logging, naming deployed resources, or integrating with external change management systems if the deployment scripts are enhanced accordingly.

## Contributing
(Details to be added - e.g., branching strategy, code review process, style guides.)

## Future Enhancements
-   [ ] Centralized testing for shared utilities.
-   [ ] More sophisticated credential management integration (e.g., HashiCorp Vault, GCP Secret Manager).
-   [ ] Full examples for GCP DataProc submission (using `gcloud` command wrapping in `run_spark_job.py`).
-   [ ] More connector types (e.g., JDBC, Kafka, Pub/Sub).
-   [ ] Advanced metadata integration (e.g., schema evolution, data catalog sync).
-   [ ] CI/CD pipeline setup.
-   [ ] More comprehensive examples for different types of Spark jobs (batch, streaming).
