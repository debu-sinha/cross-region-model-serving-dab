"""
Demo Setup for Cross-Region Model Serving.

This module creates a sample ML model with feature table for testing the
cross-region sharing workflow. It demonstrates:
1. Creating a feature table with proper constraints for online sync
2. Training a model with Feature Lookups
3. Optionally creating online tables in the source workspace
4. Deploying to a serving endpoint

Run with --cleanup true to remove all demo resources.
"""

import pandas as pd
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import serving
import mlflow
from mlflow.models import infer_signature
from sklearn.linear_model import LinearRegression
import time
from utils import (
    setup_logger,
    make_dns_compliant,
    wait_for_online_store_available,
    wait_for_endpoint_ready
)
from pyspark.sql import SparkSession

logger = setup_logger(__name__)


def wait_for_online_table_sync(online_table_name, timeout_seconds=120, poll_interval=15):
    """Wait for online table publish to initiate (simplified check)."""
    logger.info(f"Waiting for online table {online_table_name} to sync...")
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        time.sleep(poll_interval)
        logger.info(f"Online table sync in progress...")
        if time.time() - start_time > 60:
            logger.info(f"Online table {online_table_name} sync initiated")
            return True
    return True


def setup_demo(catalog_name, schema_name, model_name, table_name, create_online_table='false'):
    logger.info("Starting Demo Setup...")
    spark = SparkSession.builder.getOrCreate()

    fe = FeatureEngineeringClient()
    w = WorkspaceClient()
    
    # 1. Create Catalog and Schema if not exists
    full_schema_name = f"{catalog_name}.{schema_name}"
    try:
        w.schemas.create(name=schema_name, catalog_name=catalog_name)
        logger.info(f"Created schema {full_schema_name}")
    except Exception:
        logger.info(f"Schema {full_schema_name} might already exist.")

    # Verify schema exists
    try:
        w.schemas.get(full_name=full_schema_name)
        logger.info(f"Verified schema {full_schema_name} exists.")
    except Exception as e:
        logger.error(f"Schema {full_schema_name} does not exist or is not accessible: {e}")
        raise e

    # 2. Create Offline Feature Table with proper constraints for Online Store
    full_table_name = f"{full_schema_name}.{table_name}"
    logger.info(f"Creating Feature Table {full_table_name}...")
    
    # Create sample data - ensure primary key is not null
    pdf = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "feature_1": [1.0, 2.0, 3.0, 4.0, 5.0],
        "feature_2": [10.0, 20.0, 30.0, 40.0, 50.0],
        "target": [11.0, 22.0, 33.0, 44.0, 55.0]
    })
    df = spark.createDataFrame(pdf)
    
    # Check if table exists and handle appropriately
    created = False
    try:
        fe.create_table(
            name=full_table_name,
            primary_keys=["id"],
            df=df,
            description="Demo feature table"
        )
        logger.info(f"Created feature table {full_table_name}")
        created = True
    except Exception as e:
        if "already exists" in str(e).lower() or "ALREADY_EXISTS" in str(e):
            logger.warning(f"Feature table creation failed (might exist): {e}")
        else:
            logger.error(f"Failed to create feature table: {e}")
            raise e

    if not created:
        try:
            fe.read_table(name=full_table_name)
            logger.info("Table exists and is a valid feature table.")
        except Exception:
            logger.warning("Table exists but is not a valid feature table. Recreating...")
            try:
                w.tables.delete(full_name=full_table_name)
                fe.create_table(
                    name=full_table_name,
                    primary_keys=["id"],
                    df=df,
                    description="Demo feature table"
                )
                logger.info(f"Recreated feature table {full_table_name}")
            except Exception as e2:
                logger.error(f"Failed to recreate table: {e2}")
                raise e2

    # 3. Enable CDF and NOT NULL constraint (Required for Online Store publishing)
    logger.info("Enabling Change Data Feed and setting NOT NULL constraint on primary key...")
    try:
        spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
        logger.info("Change Data Feed enabled successfully")
    except Exception as e:
        logger.warning(f"Could not enable CDF (might already be enabled): {e}")
    
    try:
        spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN id SET NOT NULL")
        logger.info("NOT NULL constraint set on primary key")
    except Exception as e:
        logger.warning(f"Could not set NOT NULL constraint (might already be set): {e}")

    # 4. Create Online Store and Publish Table
    online_table_name = None
    online_table_created = False
    online_store_name = None
    
    if create_online_table.lower() == 'true':
        logger.info("Creating Online Store and Publishing Table...")
        online_store_name = make_dns_compliant(f"{catalog_name}-online-store")
        online_table_name = f"{full_table_name}_online"
        
        logger.info(f"Online store name (DNS compliant): {online_store_name}")
        
        try:
            # Check if Online Store exists
            store = None
            try:
                store = fe.get_online_store(name=online_store_name)
                state_str = str(store.state).upper()
                logger.info(f"Online store {online_store_name} already exists. State: {state_str}")
                
                # Check if already available
                if "AVAILABLE" in state_str:
                    logger.info(f"Online store {online_store_name} is already AVAILABLE")
                else:
                    store = wait_for_online_store_available(fe, online_store_name)
            except Exception as e:
                if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                    logger.info(f"Online store not found, creating {online_store_name}...")
                    fe.create_online_store(
                        name=online_store_name,
                        capacity="CU_1"
                    )
                    logger.info(f"Online store creation initiated. Waiting for it to become available...")
                    store = wait_for_online_store_available(fe, online_store_name)
                else:
                    raise

            # Publish Table to Online Store
            if store:
                logger.info(f"Publishing {full_table_name} to {online_table_name}...")
                try:
                    fe.publish_table(
                        online_store=store,
                        source_table_name=full_table_name,
                        online_table_name=online_table_name
                    )
                    logger.info("Table publish initiated. Sync will continue in background.")
                    wait_for_online_table_sync(online_table_name)
                    logger.info("Table published to Online Store successfully.")
                    online_table_created = True
                except Exception as pub_error:
                    error_str = str(pub_error).lower()
                    if "already exists" in error_str or "already_exists" in error_str:
                        logger.info(f"Online table {online_table_name} already exists.")
                        online_table_created = True
                    else:
                        logger.error(f"Failed to publish online table: {pub_error}")
                        raise
            
        except Exception as e:
            logger.error(f"Failed to setup Online Store: {e}")
            import traceback
            traceback.print_exc()
            logger.warning("Continuing without online table - model will be deployed without online feature lookup")
    else:
        logger.info("Skipping Online Store creation (create_online_table is false).")

    # 5. Train and Register Model
    logger.info("Training and Registering Model...")
    
    # Read the feature table
    feature_df = fe.read_table(name=full_table_name)
    pdf_full = feature_df.toPandas()
    
    # Prepare training data
    X = pdf_full[["feature_1", "feature_2"]]
    y = pdf_full["target"]
    
    # Train model
    model = LinearRegression()
    model.fit(X, y)
    
    mlflow.set_registry_uri("databricks-uc")
    full_model_name = f"{full_schema_name}.{model_name}"
    
    # Set experiment
    current_user = w.current_user.me().user_name
    experiment_path = f"/Users/{current_user}/cross_region_demo_{model_name}"
    logger.info(f"Setting MLflow experiment to {experiment_path}")
    mlflow.set_experiment(experiment_path)
    
    with mlflow.start_run() as run:
        if online_table_created:
            # Log model WITH feature store integration (online table available)
            logger.info("Logging model WITH feature store integration (online table available)")
            
            # Create a lookup DataFrame with just the primary key and label
            lookup_df = feature_df.select("id", "target")
            
            # Define Feature Lookups for automatic feature lookup at serving time
            feature_lookups = [
                FeatureLookup(
                    table_name=full_table_name,
                    feature_names=["feature_1", "feature_2"],
                    lookup_key="id"
                )
            ]
            
            # Create training set with feature lookups
            training_set = fe.create_training_set(
                df=lookup_df,
                feature_lookups=feature_lookups,
                label="target",
                exclude_columns=["id"]
            )
            
            # Let Feature Engineering client auto-infer signature from training set
            # This avoids signature mismatch warnings
            fe.log_model(
                model=model,
                artifact_path="model",
                flavor=mlflow.sklearn,
                training_set=training_set,
                registered_model_name=full_model_name,
                infer_input_example=True
            )
            logger.info("Model logged with feature store integration - endpoint will auto-lookup features")
        else:
            # Log model WITHOUT feature store integration
            logger.info("Logging model WITHOUT feature store integration (online table not available)")
            
            # Standard input with all features
            input_example = pd.DataFrame({
                "feature_1": [1.0, 2.0, 3.0],
                "feature_2": [10.0, 20.0, 30.0]
            })
            predictions = model.predict(input_example)
            signature = infer_signature(input_example, predictions)
            
            mlflow.sklearn.log_model(
                model,
                artifact_path="model",
                registered_model_name=full_model_name,
                input_example=input_example,
                signature=signature
            )
            logger.info("Model logged without feature store integration - endpoint requires all features in request")
            
        run_id = run.info.run_id
        
    logger.info(f"Model registered as {full_model_name}")
    logger.info(f"Run ID: {run_id}")

    # 6. Deploy to Serving Endpoint
    endpoint_name = f"source-{model_name}-endpoint"
    logger.info(f"Deploying to Source Endpoint: {endpoint_name}")
    
    # Get latest version
    client = mlflow.MlflowClient()
    versions = client.search_model_versions(f"name='{full_model_name}'")
    if not versions:
        logger.error(f"No versions found for model {full_model_name}")
        raise ValueError(f"No versions found for model {full_model_name}")
    latest_version = str(sorted(versions, key=lambda x: int(x.version), reverse=True)[0].version)
    logger.info(f"Deploying version {latest_version} of model {full_model_name}")

    served_entities = [
        serving.ServedEntityInput(
            entity_name=full_model_name,
            entity_version=latest_version,
            workload_size="Small",
            scale_to_zero_enabled=True
        )
    ]

    endpoint_exists = False
    try:
        existing_endpoint = w.serving_endpoints.get(endpoint_name)
        endpoint_exists = True
        logger.info(f"Endpoint {endpoint_name} already exists.")
    except Exception:
        logger.info(f"Endpoint {endpoint_name} does not exist.")

    if endpoint_exists:
        # Wait for endpoint to be ready before updating
        wait_for_endpoint_ready(w, endpoint_name, timeout_seconds=300)
        
        logger.info("Updating endpoint configuration...")
        try:
            w.serving_endpoints.update_config(
                name=endpoint_name,
                served_entities=served_entities
            )
            logger.info("Endpoint update initiated.")
        except Exception as update_error:
            error_str = str(update_error).lower()
            if "conflict" in error_str or "in progress" in error_str:
                logger.warning(f"Endpoint is being updated, waiting and retrying...")
                time.sleep(60)  # Wait a minute
                try:
                    w.serving_endpoints.update_config(
                        name=endpoint_name,
                        served_entities=served_entities
                    )
                    logger.info("Endpoint update initiated (retry successful).")
                except Exception as retry_error:
                    logger.error(f"Failed to update endpoint after retry: {retry_error}")
            else:
                logger.error(f"Failed to update endpoint: {update_error}")
    else:
        logger.info("Creating new endpoint...")
        try:
            w.serving_endpoints.create(
                name=endpoint_name,
                config=serving.EndpointCoreConfigInput(
                    name=endpoint_name,  # Required parameter
                    served_entities=served_entities
                )
            )
            logger.info("Endpoint creation initiated.")
        except Exception as e:
            logger.error(f"Failed to create endpoint: {e}")
            import traceback
            traceback.print_exc()
    
    # Print sample inference request
    logger.info("\n" + "="*60)
    logger.info("DEPLOYMENT SUMMARY")
    logger.info("="*60)
    logger.info(f"Endpoint: {endpoint_name}")
    logger.info(f"Model: {full_model_name} (version {latest_version})")
    
    if online_table_created:
        logger.info(f"Online Store: {online_store_name}")
        logger.info(f"Online Table: {online_table_name}")
        logger.info("\nSAMPLE REQUEST (features auto-looked up from online store):")
        logger.info('''
{
    "inputs": [
        {"id": 1},
        {"id": 2},
        {"id": 3}
    ]
}
''')
    else:
        logger.info("Online Table: NOT CREATED")
        logger.info("\nSAMPLE REQUEST (all features required in request):")
        logger.info('''
{
    "inputs": [
        {"feature_1": 1.0, "feature_2": 10.0},
        {"feature_1": 2.0, "feature_2": 20.0},
        {"feature_1": 3.0, "feature_2": 30.0}
    ]
}
''')
    logger.info("="*60)


def cleanup_demo(catalog_name, schema_name, model_name, table_name):
    logger.info("Cleaning up demo resources...")
    fe = FeatureEngineeringClient()
    w = WorkspaceClient()
    
    full_schema_name = f"{catalog_name}.{schema_name}"
    full_model_name = f"{full_schema_name}.{model_name}"
    full_table_name = f"{full_schema_name}.{table_name}"
    serving_endpoint_name = f"source-{model_name}-endpoint"
    online_store_name = make_dns_compliant(f"{catalog_name}-online-store")
    online_table_name = f"{full_table_name}_online"

    # Delete Endpoint
    try:
        w.serving_endpoints.delete(name=serving_endpoint_name)
        logger.info(f"Deleted endpoint {serving_endpoint_name}")
    except Exception as e:
        logger.warning(f"Failed to delete endpoint: {e}")

    # Delete Online Table (unpublish) - if published
    try:
        logger.info(f"Note: Online table {online_table_name} will be cleaned up with online store")
    except Exception as e:
        logger.warning(f"Note on online table cleanup: {e}")

    # Delete Online Store (if exists)
    try:
        fe.delete_online_store(name=online_store_name)
        logger.info(f"Deleted online store {online_store_name}")
    except Exception as e:
        logger.warning(f"Failed to delete online store: {e}")

    # Delete Model
    try:
        w.registered_models.delete(full_name=full_model_name)
        logger.info(f"Deleted model {full_model_name}")
    except Exception as e:
        logger.warning(f"Failed to delete model: {e}")

    # Delete Table
    try:
        w.tables.delete(full_name=full_table_name)
        logger.info(f"Deleted table {full_table_name}")
    except Exception as e:
        logger.warning(f"Failed to delete table: {e}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Demo setup for Feature Store with Online Tables")
    parser.add_argument("--model_name", required=True, help="Full model name (catalog.schema.model)")
    parser.add_argument("--table", required=True, help="Table name for feature table")
    parser.add_argument("--create_online_table", help="Whether to create online table (true/false)", default="false")
    parser.add_argument("--cleanup", help="Whether to cleanup resources (true/false)", default="false")
    args = parser.parse_args()
    
    parts = args.model_name.split('.')
    if len(parts) != 3:
        raise ValueError("model_name must be in the format catalog.schema.model")
    
    catalog, schema, model = parts[0], parts[1], parts[2]
    
    if args.cleanup.lower() == 'true':
        cleanup_demo(catalog, schema, model, args.table)
    else:
        setup_demo(catalog, schema, model, args.table, args.create_online_table)


if __name__ == "__main__":
    main()