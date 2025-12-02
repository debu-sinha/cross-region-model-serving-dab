import argparse
import sys
from source_manager import SourceManager
from target_manager import TargetManager
from utils import setup_logger, AppConfig

logger = setup_logger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Cross Region Model Serving Manager")
    parser.add_argument("--mode", choices=["source", "target"], required=True, help="Mode of operation")
    
    # Source Arguments
    parser.add_argument("--model_name", help="Full name of the model in source (catalog.schema.model)")
    parser.add_argument("--share_name", help="Name of the Delta Share")
    parser.add_argument("--recipient_name", help="Name of the Recipient")
    parser.add_argument("--create_recipient", help="Whether to create recipient (true/false)", default="true")
    
    # Target Arguments
    parser.add_argument("--provider_name", help="Name of the Provider in target")
    parser.add_argument("--target_catalog", help="Name of the catalog to create in target")
    parser.add_argument("--target_host", help="Target Workspace Host URL")
    parser.add_argument("--target_token", help="Target Workspace PAT")
    parser.add_argument("--deploy_serving", help="Whether to deploy serving endpoint (true/false)", default="false")
    parser.add_argument("--create_online_table", help="Whether to create online table (true/false)", default="false")
    parser.add_argument("--serving_endpoint_name", help="Name of the serving endpoint in target")
    parser.add_argument("--online_store_name", help="Name of the online store")
    parser.add_argument("--create_online_store", help="Whether to create online store (true/false)", default="true")
    parser.add_argument("--online_table_target_catalog", help="Target catalog for online tables")
    parser.add_argument("--online_table_target_schema", help="Target schema for online tables")
    parser.add_argument("--use_existing_catalog", help="Use existing catalog instead of creating (true/false)", default="false")

    args = parser.parse_args()
    
    try:
        config = AppConfig.from_args(args)
        config.validate()
    except ValueError as e:
        logger.error(f"Configuration Error: {e}")
        sys.exit(1)

    if config.mode == "source":
        manager = SourceManager()
        manager.run(
            config.model_name, 
            config.share_name, 
            config.recipient_name,
            target_host=config.target_host,
            target_token=config.target_token,
            create_recipient=args.create_recipient.lower() == 'true'
        )

    elif config.mode == "target":
        manager = TargetManager(config.target_host, config.target_token)
        
        # Handle NONE defaults from DAB
        online_store_name = args.online_store_name if args.online_store_name and args.online_store_name != "NONE" else None
        online_table_target_catalog = args.online_table_target_catalog if args.online_table_target_catalog and args.online_table_target_catalog != "NONE" else None
        online_table_target_schema = args.online_table_target_schema if args.online_table_target_schema and args.online_table_target_schema != "NONE" else None

        manager.run(
            config.share_name,
            config.provider_name,
            config.target_catalog,
            config.deploy_serving,
            config.serving_endpoint_name,
            create_online_table=config.create_online_table,
            online_store_name=online_store_name,
            create_online_store=args.create_online_store.lower() == 'true',
            online_table_target_catalog=online_table_target_catalog,
            online_table_target_schema=online_table_target_schema,
            use_existing_catalog=args.use_existing_catalog.lower() == 'true'
        )

if __name__ == "__main__":
    main()
