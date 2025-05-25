import os
import json

# Placeholder for actual credential store (e.g., GitHub Secrets, HashiCorp Vault)
# For local development, you might use environment variables or a local JSON file.
# IMPORTANT: Do not commit actual secrets to the repository.
# This file provides an abstraction for credential retrieval.

def get_secret(secret_name: str) -> str | None:
    """
    Retrieves a secret by its name.

    This is a placeholder implementation. In a production environment,
    this function would integrate with a secure secret management system
    like GitHub Secrets (if running in GitHub Actions), HashiCorp Vault,
    AWS Secrets Manager, or GCP Secret Manager.

    For GitHub Secrets, you would typically access them as environment
    variables within a GitHub Actions workflow.

    Args:
        secret_name (str): The name of the secret to retrieve (e.g., "DB_PASSWORD").

    Returns:
        str | None: The secret value if found, otherwise None.
    """
    # Option 1: Try to get from environment variables (common for GitHub Actions)
    value = os.environ.get(secret_name)
    if value:
        print(f"Retrieved secret '{secret_name}' from environment variable.")
        return value

    # Option 2: Placeholder for local development - a dummy JSON file (NOT FOR PRODUCTION)
    # Create a file named '.local_secrets.json' in the project root for local testing.
    # Ensure '.local_secrets.json' is in .gitignore
    # Example '.local_secrets.json':
    # {
    #   "DB_USER": "test_user",
    #   "DB_PASSWORD": "test_password123"
    # }
    local_secrets_file = os.path.join(os.path.dirname(__file__), '..', '.local_secrets.json') # Assuming this file is in shared_utils
    if os.path.exists(local_secrets_file):
        try:
            with open(local_secrets_file, 'r') as f:
                secrets = json.load(f)
            value = secrets.get(secret_name)
            if value:
                print(f"Retrieved secret '{secret_name}' from local '.local_secrets.json'. (FOR DEVELOPMENT ONLY)")
                return value
        except json.JSONDecodeError:
            print(f"Error decoding '{local_secrets_file}'. Make sure it's valid JSON.")
        except Exception as e:
            print(f"Error reading '{local_secrets_file}': {e}")

    print(f"Warning: Secret '{secret_name}' not found. Ensure it's configured in your environment or '.local_secrets.json' for local development.")
    return None

if __name__ == '__main__':
    # Example Usage (assuming you have set up environment variables or .local_secrets.json)

    # To test with environment variables:
    # export MY_API_KEY="env_secret_value"
    # python shared_utils/credential_utils.py

    # To test with .local_secrets.json (create this file in the project root, one level above shared_utils):
    # { "MY_API_KEY": "local_secret_value", "DB_PASSWORD": "local_db_password" }
    # python shared_utils/credential_utils.py

    api_key = get_secret("MY_API_KEY")
    if api_key:
        print(f"Successfully retrieved MY_API_KEY: {api_key}")
    else:
        print("MY_API_KEY not found.")

    db_password = get_secret("DB_PASSWORD")
    if db_password:
        print(f"Successfully retrieved DB_PASSWORD: {'*' * len(db_password)}") # Avoid printing actual password
    else:
        print("DB_PASSWORD not found.")

    non_existent_secret = get_secret("NON_EXISTENT_SECRET")
    if not non_existent_secret:
        print("NON_EXISTENT_SECRET was correctly not found.")
