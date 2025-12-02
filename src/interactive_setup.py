import argparse
import sys
from source_manager import SourceManager
from target_manager import TargetManager
from utils import setup_logger, AppConfig

logger = setup_logger(__name__)

def get_input(prompt, default=None, required=True):
    """Helper to get user input with optional default."""
    if default:
        prompt_text = f"{prompt} [{default}]: "
    else:
        prompt_text = f"{prompt}: "
    
    while True:
        value = input(prompt_text).strip()
        if not value and default:
            return default
        if not value and required:
            print("Value is required.")
            continue
        return value

def get_yes_no(prompt, default="y"):
    """Helper to get yes/no input."""
    prompt_text = f"{prompt} (y/n) [{default}]: "
    while True:
        value = input(prompt_text).strip().lower()
        if not value:
            value = default
        if value in ["y", "yes"]:
            return True
        if value in ["n", "no"]:
            return False
        print("Please enter 'y' or 'n'.")

def run_source_interactive():
    print("\n=== Source Setup Interactive Mode ===\n")
    
    model_name = get_input("Enter full model name (catalog.schema.model)")
    share_name = get_input("Enter Delta Share name", default="demo_share")
    
    create_recipient = True
    recipient_name = get_input("Enter Recipient name", default="demo_recipient")
    
    # Check if user wants to use existing recipient
    if get_yes_no(f"Do you want to create a NEW recipient named '{recipient_name}'?", default="y"):
        create_recipient = True
    else:
        create_recipient = False
        print(f"Using EXISTING recipient '{recipient_name}'.")

    target_host = get_input("Enter Target Workspace Host (optional, for D2D)", required=False)
    target_token = None
    if target_host:
        target_token = get_input("Enter Target Workspace Token", required=True)

    print("\nStarting Source Setup...")
    manager = SourceManager()
    manager.run(
        model_name=model_name,
        share_name=share_name,
        recipient_name=recipient_name,
        target_host=target_host,
        target_token=target_token,
        create_recipient=create_recipient
    )

def run_target_interactive():
    print("\n=== Target Setup Interactive Mode ===\n")
    
    target_host = get_input("Enter Target Workspace Host", required=True)
    target_token = get_input("Enter Target Workspace Token", required=True)
    
    share_name = get_input("Enter Share Name (from source)", default="demo_share")
    provider_name = get_input("Enter Provider Name", default="db_demos_provider")
    
    target_catalog = get_input("Enter name for the mounted catalog", default="shared_model_catalog")
    
    deploy_serving = get_yes_no("Do you want to deploy a serving endpoint?", default="n")
    serving_endpoint_name = None
    if deploy_serving:
        serving_endpoint_name = get_input("Enter Serving Endpoint Name", default="demo_endpoint")

    create_online_table = get_yes_no("Do you want to create online tables?", default="n")
    
    online_store_name = None
    create_online_store = True
    online_table_target_catalog = None
    online_table_target_schema = None

    if create_online_table:
        # Online Store Configuration
        use_existing_store = get_yes_no("Do you want to use an EXISTING Online Store?", default="n")
        if use_existing_store:
            online_store_name = get_input("Enter Existing Online Store Name")
            create_online_store = False
        else:
            online_store_name = get_input("Enter New Online Store Name", default=f"{target_catalog}-online-store".replace('_', '-'))
            create_online_store = True
            
        # Online Table Location
        # Since the shared catalog is read-only, we usually need a separate catalog for online tables
        # unless the user has some special setup.
        print("\nNote: Online tables cannot be created inside a read-only shared catalog.")
        use_custom_location = get_yes_no("Do you want to specify a custom Catalog and Schema for the online table?", default="y")
        
        if use_custom_location:
            online_table_target_catalog = get_input("Enter Target Catalog for Online Table")
            online_table_target_schema = get_input("Enter Target Schema for Online Table")
        else:
            print("Warning: Attempting to create online table in the source location (might fail if read-only).")

    print("\nStarting Target Setup...")
    manager = TargetManager(target_host, target_token)
    manager.run(
        share_name=share_name,
        provider_name=provider_name,
        target_catalog=target_catalog,
        deploy_serving=deploy_serving,
        serving_endpoint_name=serving_endpoint_name,
        create_online_table=create_online_table,
        online_store_name=online_store_name,
        create_online_store=create_online_store,
        online_table_target_catalog=online_table_target_catalog,
        online_table_target_schema=online_table_target_schema
    )

def main():
    print("Welcome to the Cross-Region Model Serving Interactive Setup")
    mode = get_input("Select Mode (source/target)", default="source").lower()
    
    if mode == "source":
        run_source_interactive()
    elif mode == "target":
        run_target_interactive()
    else:
        print("Invalid mode. Please choose 'source' or 'target'.")
        sys.exit(1)

if __name__ == "__main__":
    main()
