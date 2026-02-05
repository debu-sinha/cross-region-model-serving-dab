"""
Target Manager for Cross-Region Model Serving.

This module handles the target workspace setup for consuming Delta Shared models:
1. Create/use catalog from Delta Share
2. Discover shared model and its feature table dependencies
3. Create online store and publish feature tables (BEFORE endpoint deployment)
4. Wait for online tables to be ONLINE (critical for feature lookup)
5. Deploy model to serving endpoint

IMPORTANT: Online tables must be ready BEFORE endpoint deployment for models
with Feature Lookups, otherwise the endpoint will fail with
"Online feature store setup failed".
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import serving
from utils import (
    setup_logger,
    make_dns_compliant,
    is_databricks_runtime,
    wait_for_online_store_available,
    wait_for_online_table_ready,
    wait_for_endpoint_ready,
    validate_target_config,
    ValidationError
)
import time
import os
from contextlib import contextmanager

logger = setup_logger(__name__)


class TargetManager:
    """
    Manages target workspace setup for consuming shared models.

    The run() method executes the following phases in order:
    1. Catalog Setup - Create or verify shared catalog
    2. Model Discovery - Find model and dependencies in shared catalog
    3. Online Table Setup - Create online store and tables (MUST complete before endpoint)
    4. Endpoint Deployment - Deploy model to serving endpoint
    """

    def __init__(self, host, token):
        # Resolve any secret references before using credentials
        self.host = self._resolve_secret(host)
        self.token = self._resolve_secret(token)
        self.w = WorkspaceClient(host=self.host, token=self.token)

    @staticmethod
    def _resolve_secret(value):
        """
        Resolve secret references in the format {{secrets/scope/key}}.

        When running on a Databricks cluster, uses dbutils.secrets.get().
        Returns the original value if not a secret reference or not on cluster.
        """
        if not value or not isinstance(value, str):
            return value

        # Check if it's a secret reference
        import re
        match = re.match(r'\{\{secrets/([^/]+)/([^}]+)\}\}', value)
        if not match:
            return value

        scope = match.group(1)
        key = match.group(2)

        try:
            # Try to get dbutils (only available on Databricks clusters)
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            dbutils = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils()
            secret_value = dbutils.secrets().get(scope, key)
            logger.info(f"Resolved secret from scope '{scope}', key '{key}'")
            return secret_value
        except Exception as e:
            logger.warning(f"Could not resolve secret {{{{secrets/{scope}/{key}}}}}: {e}")
            logger.warning("Secret resolution requires running on a Databricks cluster")
            return value

    @contextmanager
    def _target_fe_client(self):
        """Context manager to temporarily set env vars for FeatureEngineeringClient."""
        if not is_databricks_runtime():
            logger.warning("FeatureEngineeringClient requires Databricks Runtime.")
            yield None
            return

        original_host = os.environ.get('DATABRICKS_HOST')
        original_token = os.environ.get('DATABRICKS_TOKEN')

        try:
            os.environ['DATABRICKS_HOST'] = self.host
            os.environ['DATABRICKS_TOKEN'] = self.token

            from databricks.feature_engineering import FeatureEngineeringClient
            yield FeatureEngineeringClient()
        finally:
            if original_host:
                os.environ['DATABRICKS_HOST'] = original_host
            else:
                os.environ.pop('DATABRICKS_HOST', None)

            if original_token:
                os.environ['DATABRICKS_TOKEN'] = original_token
            else:
                os.environ.pop('DATABRICKS_TOKEN', None)

    def run(self, share_name, provider_name, target_catalog, deploy_serving, serving_endpoint_name,
            create_online_table=False, online_store_name=None, create_online_store=True,
            online_table_target_catalog=None, online_table_target_schema=None,
            use_existing_catalog=False):
        """
        Run the target manager to consume a Delta Share.

        IMPORTANT: The execution order is critical for models with Feature Lookups:
        1. Create/verify catalog
        2. Find model and its feature table dependencies
        3. Create online tables and WAIT until they are ONLINE
        4. Only then deploy the serving endpoint

        Args:
            share_name: Name of the share to consume
            provider_name: Name of the provider (e.g., 'self' for same-metastore)
            target_catalog: Name of the catalog to create or use
            deploy_serving: Whether to deploy a serving endpoint
            serving_endpoint_name: Name for the serving endpoint
            create_online_table: Whether to create online tables
            online_store_name: Name of the online store (will be made DNS-compliant)
            create_online_store: Whether to create online store if it doesn't exist
            online_table_target_catalog: Catalog for online tables (required - shared catalogs are read-only)
            online_table_target_schema: Schema for online tables
            use_existing_catalog: If True, use an existing catalog instead of creating one.
        """
        logger.info("=" * 60)
        logger.info("STARTING TARGET MANAGER")
        logger.info("=" * 60)

        # Validate configuration
        if create_online_table:
            try:
                validate_target_config(
                    share_name, provider_name, target_catalog,
                    create_online_table, online_table_target_catalog, online_table_target_schema
                )
            except ValidationError as e:
                logger.error(str(e))
                return

        # =========================================================================
        # PHASE 1: CATALOG SETUP
        # =========================================================================
        logger.info("")
        logger.info("PHASE 1: Catalog Setup")
        logger.info("-" * 40)

        if not self._setup_catalog(share_name, provider_name, target_catalog, use_existing_catalog):
            return

        # =========================================================================
        # PHASE 2: MODEL DISCOVERY
        # =========================================================================
        logger.info("")
        logger.info("PHASE 2: Model Discovery")
        logger.info("-" * 40)

        model_info = self._discover_model(target_catalog)
        if not model_info:
            logger.warning("No model found in shared catalog. Nothing to deploy.")
            return

        found_model_name = model_info["model_name"]
        found_model_version = model_info["version"]
        feature_tables = model_info["feature_tables"]

        # =========================================================================
        # PHASE 3: ONLINE TABLE SETUP (MUST complete before endpoint deployment)
        # =========================================================================
        online_tables_ready = False

        if create_online_table and feature_tables:
            logger.info("")
            logger.info("PHASE 3: Online Table Setup")
            logger.info("-" * 40)
            logger.info(f"Model has {len(feature_tables)} feature table dependency(s)")
            logger.info("Online tables MUST be ready before endpoint deployment")

            online_tables_ready = self._setup_online_tables(
                feature_tables=feature_tables,
                online_store_name=online_store_name,
                create_online_store=create_online_store,
                online_table_target_catalog=online_table_target_catalog,
                online_table_target_schema=online_table_target_schema,
                target_catalog=target_catalog
            )

            if not online_tables_ready:
                logger.error("Online tables are not ready. Endpoint deployment may fail.")
                logger.error("For models with Feature Lookups, online tables must be ONLINE before serving.")

        elif create_online_table and not feature_tables:
            logger.info("")
            logger.info("PHASE 3: Online Table Setup (Skipped)")
            logger.info("-" * 40)
            logger.info("Model has no feature table dependencies - skipping online table creation")
            online_tables_ready = True  # No tables needed

        else:
            logger.info("")
            logger.info("PHASE 3: Online Table Setup (Disabled)")
            logger.info("-" * 40)
            logger.info("create_online_table=false - skipping online table setup")
            if feature_tables:
                logger.warning(f"WARNING: Model has {len(feature_tables)} feature table dependencies!")
                logger.warning("Endpoint deployment will likely fail without online tables.")

        # =========================================================================
        # PHASE 4: ENDPOINT DEPLOYMENT
        # =========================================================================
        if deploy_serving:
            logger.info("")
            logger.info("PHASE 4: Endpoint Deployment")
            logger.info("-" * 40)

            if feature_tables and not online_tables_ready:
                logger.error("Cannot deploy endpoint - online tables are not ready")
                logger.error("Please ensure online tables are ONLINE before deploying")
                return

            self._deploy_endpoint(
                found_model_name, found_model_version,
                serving_endpoint_name, online_tables_ready
            )
        else:
            logger.info("")
            logger.info("PHASE 4: Endpoint Deployment (Skipped)")
            logger.info("-" * 40)
            logger.info("deploy_serving=false - skipping endpoint deployment")

        # =========================================================================
        # SUMMARY
        # =========================================================================
        self._print_summary(
            target_catalog, found_model_name, found_model_version,
            deploy_serving, serving_endpoint_name,
            create_online_table, online_store_name,
            online_table_target_catalog, online_table_target_schema,
            online_tables_ready
        )

    def _setup_catalog(self, share_name, provider_name, target_catalog, use_existing_catalog):
        """Phase 1: Create or verify the shared catalog."""
        if use_existing_catalog:
            logger.info(f"Using existing catalog: {target_catalog}")
            try:
                existing_cat = self.w.catalogs.get(target_catalog)
                logger.info(f"  Catalog found. Type: {existing_cat.catalog_type}")

                if existing_cat.catalog_type and 'DELTASHARING' in str(existing_cat.catalog_type):
                    logger.info(f"  Provider: {existing_cat.provider_name}")
                    logger.info(f"  Share: {existing_cat.share_name}")
                else:
                    logger.info("  This is a regular catalog (not from Delta Sharing)")
                return True

            except Exception as e:
                logger.error(f"Catalog '{target_catalog}' not found: {e}")
                logger.error("Cannot proceed - catalog does not exist and use_existing_catalog=True")
                logger.error("Either create the catalog first or set use_existing_catalog=False")
                return False
        else:
            logger.info(f"Creating catalog '{target_catalog}' from share '{share_name}'")
            try:
                existing_cat = self.w.catalogs.get(target_catalog)
                logger.info(f"  Catalog already exists. Type: {existing_cat.catalog_type}")
                return True
            except Exception:
                try:
                    self.w.catalogs.create(
                        name=target_catalog,
                        provider_name=provider_name,
                        share_name=share_name
                    )
                    logger.info(f"  Created catalog '{target_catalog}'")
                    return True
                except Exception as e:
                    logger.error(f"Failed to create catalog: {e}")
                    logger.error("This often happens if:")
                    logger.error("  - The Provider is not defined or Recipient not activated")
                    logger.error("  - You don't have CREATE CATALOG permission")
                    logger.error("")
                    logger.error("Remediation:")
                    logger.error("  1. Ask an admin to create a shared catalog for you")
                    logger.error("  2. Use --use_existing_catalog with an existing catalog name")
                    return False

    def _discover_model(self, target_catalog):
        """Phase 2: Find model and its feature table dependencies in the shared catalog."""
        logger.info(f"Searching for models in catalog '{target_catalog}'...")

        found_model_name = None
        found_model_version = None
        feature_tables = []

        try:
            schemas = list(self.w.schemas.list(catalog_name=target_catalog))
            for schema in schemas:
                if schema.name == 'information_schema':
                    continue

                logger.info(f"  Scanning schema: {schema.name}")
                try:
                    models = list(self.w.registered_models.list(
                        catalog_name=target_catalog,
                        schema_name=schema.name
                    ))

                    for model in models:
                        logger.info(f"  Found model: {model.full_name}")
                        found_model_name = model.full_name

                        # Get the latest version
                        try:
                            versions = list(self.w.model_versions.list(full_name=model.full_name))
                            if versions:
                                latest = sorted(versions, key=lambda x: int(x.version), reverse=True)[0]
                                found_model_version = str(latest.version)
                                logger.info(f"    Latest version: {found_model_version}")

                                # Extract feature table dependencies
                                mv = self.w.model_versions.get(
                                    full_name=model.full_name,
                                    version=found_model_version
                                )
                                if mv.model_version_dependencies and mv.model_version_dependencies.dependencies:
                                    for dep in mv.model_version_dependencies.dependencies:
                                        if dep.table and dep.table.table_full_name:
                                            ft_name = dep.table.table_full_name
                                            if ft_name not in feature_tables:
                                                feature_tables.append(ft_name)
                                                logger.info(f"    Feature dependency: {ft_name}")
                            else:
                                found_model_version = "1"
                                logger.warning("    No versions found, defaulting to version 1")
                        except Exception as ve:
                            logger.warning(f"    Could not get versions: {ve}")
                            found_model_version = "1"

                        break  # Use first model found
                except Exception as me:
                    logger.warning(f"  Error listing models in {schema.name}: {me}")

                if found_model_name:
                    break

        except Exception as e:
            logger.error(f"Error scanning for models: {e}")

        if found_model_name:
            return {
                "model_name": found_model_name,
                "version": found_model_version,
                "feature_tables": feature_tables
            }
        return None

    def _setup_online_tables(self, feature_tables, online_store_name, create_online_store,
                             online_table_target_catalog, online_table_target_schema, target_catalog):
        """
        Phase 3: Create online store and publish feature tables.

        CRITICAL: This must complete before endpoint deployment.

        Returns:
            bool: True if all online tables are ready, False otherwise
        """
        if not is_databricks_runtime():
            logger.warning("Not running in Databricks Runtime")
            logger.warning("Online table creation requires FeatureEngineeringClient")
            logger.warning("Please run this on a Databricks cluster")
            return False

        with self._target_fe_client() as fe:
            if fe is None:
                logger.error("Could not initialize FeatureEngineeringClient")
                return False

            # Generate DNS-compliant online store name
            # Use the online_table_target_catalog (writable catalog) for naming, not target_catalog (shared/read-only)
            if not online_store_name or online_store_name == "NONE":
                online_store_name = make_dns_compliant(f"{online_table_target_catalog}-online-store")
            else:
                online_store_name = make_dns_compliant(online_store_name)

            logger.info(f"Online store name: {online_store_name}")

            # Step 1: Ensure online store exists and is available
            store = self._ensure_online_store(fe, online_store_name, create_online_store)
            if not store:
                return False

            # Step 2: Publish each feature table and wait for it to be ready
            all_tables_ready = True
            created_online_tables = []

            for ft_name in feature_tables:
                table_short_name = ft_name.split(".")[-1]

                # The source table is in the shared catalog
                # But we need to reference it correctly
                source_parts = ft_name.split(".")
                if len(source_parts) == 3:
                    # Replace source catalog with target shared catalog
                    source_table_in_shared = f"{target_catalog}.{source_parts[1]}.{source_parts[2]}"
                else:
                    source_table_in_shared = ft_name

                # Online table goes in the writable target location
                online_table_name = f"{online_table_target_catalog}.{online_table_target_schema}.{table_short_name}_online"

                logger.info(f"Publishing '{source_table_in_shared}' -> '{online_table_name}'")

                try:
                    # Publish the table
                    fe.publish_table(
                        online_store=store,
                        source_table_name=source_table_in_shared,
                        online_table_name=online_table_name
                    )
                    logger.info(f"  Publish initiated for {online_table_name}")
                    created_online_tables.append(online_table_name)

                except Exception as pub_e:
                    error_str = str(pub_e).lower()
                    if "already exists" in error_str or "already_exists" in error_str:
                        logger.info(f"  Online table '{online_table_name}' already exists")
                        created_online_tables.append(online_table_name)
                    else:
                        logger.error(f"  Failed to publish: {pub_e}")
                        all_tables_ready = False

            # Step 3: Wait for all online tables to be ONLINE
            if created_online_tables:
                logger.info("")
                logger.info("Waiting for online tables to be ONLINE...")
                logger.info("(This is REQUIRED before endpoint deployment)")

                for online_table_name in created_online_tables:
                    is_ready = wait_for_online_table_ready(
                        self.w, online_table_name,
                        timeout_seconds=600,
                        poll_interval=30
                    )
                    if not is_ready:
                        logger.error(f"Online table '{online_table_name}' is not ready")
                        all_tables_ready = False

            if all_tables_ready:
                logger.info("All online tables are ONLINE and ready for serving")
            else:
                logger.error("Some online tables failed to reach ONLINE state")

            return all_tables_ready

    def _ensure_online_store(self, fe, online_store_name, create_online_store):
        """Ensure online store exists and is available."""
        try:
            store = fe.get_online_store(name=online_store_name)

            # Handle case where store exists but state might be None
            if store is None:
                raise ValueError(f"Online store '{online_store_name}' returned None")

            state_str = str(store.state).upper() if store.state else "UNKNOWN"
            logger.info(f"Online store '{online_store_name}' exists. State: {state_str}")

            if "AVAILABLE" not in state_str and state_str != "UNKNOWN":
                store = wait_for_online_store_available(fe, online_store_name)
            elif state_str == "UNKNOWN":
                # State is unknown, try waiting for it to become available
                logger.info("Online store state unknown, waiting for it to become AVAILABLE...")
                store = wait_for_online_store_available(fe, online_store_name)
            return store

        except Exception as e:
            error_str = str(e).lower()
            if "not found" in error_str or "does not exist" in error_str:
                if create_online_store:
                    logger.info(f"Creating online store '{online_store_name}'...")
                    fe.create_online_store(name=online_store_name, capacity="CU_1")
                    logger.info("  Creation initiated. Waiting for AVAILABLE state...")
                    return wait_for_online_store_available(fe, online_store_name)
                else:
                    logger.error(f"Online store '{online_store_name}' not found and create_online_store=False")
                    return None
            else:
                logger.error(f"Error checking online store: {e}")
                return None

    def _deploy_endpoint(self, model_name, model_version, endpoint_name, has_online_tables):
        """Phase 4: Deploy model to serving endpoint."""
        if not endpoint_name:
            # Generate default endpoint name
            model_short = model_name.split('.')[-1]
            endpoint_name = f"target-{make_dns_compliant(model_short)}-endpoint"
            logger.info(f"Using generated endpoint name: {endpoint_name}")

        logger.info(f"Deploying model to endpoint: {endpoint_name}")
        logger.info(f"  Model: {model_name} v{model_version}")

        # Check if endpoint exists
        endpoint_exists = False
        try:
            existing = self.w.serving_endpoints.get(endpoint_name)
            endpoint_exists = True
            logger.info(f"  Endpoint exists. Current state: {existing.state.ready if existing.state else 'unknown'}")
        except Exception:
            logger.info("  Endpoint does not exist. Will create new.")

        # Configure served entity
        entity = serving.ServedEntityInput(
            entity_name=model_name,
            entity_version=model_version,
            workload_size="Small",
            scale_to_zero_enabled=True
        )

        if endpoint_exists:
            # Wait for endpoint to be ready before updating
            wait_for_endpoint_ready(self.w, endpoint_name, timeout_seconds=300)

            logger.info("  Updating endpoint configuration...")
            try:
                self.w.serving_endpoints.update_config(
                    name=endpoint_name,
                    served_entities=[entity]
                )
                logger.info("  Endpoint update initiated")
            except Exception as e:
                error_str = str(e).lower()
                if "conflict" in error_str or "in progress" in error_str:
                    logger.warning("  Endpoint busy. Waiting 60s and retrying...")
                    time.sleep(60)
                    try:
                        self.w.serving_endpoints.update_config(
                            name=endpoint_name,
                            served_entities=[entity]
                        )
                        logger.info("  Endpoint update initiated (retry successful)")
                    except Exception as retry_e:
                        logger.error(f"  Failed to update endpoint: {retry_e}")
                else:
                    logger.error(f"  Failed to update endpoint: {e}")
        else:
            logger.info("  Creating new endpoint...")
            try:
                self.w.serving_endpoints.create(
                    name=endpoint_name,
                    config=serving.EndpointCoreConfigInput(
                        name=endpoint_name,
                        served_entities=[entity]
                    )
                )
                logger.info("  Endpoint creation initiated")
            except Exception as e:
                logger.error(f"  Failed to create endpoint: {e}")
                import traceback
                traceback.print_exc()

    def _print_summary(self, target_catalog, model_name, model_version,
                       deploy_serving, endpoint_name, create_online_table,
                       online_store_name, online_table_target_catalog,
                       online_table_target_schema, online_tables_ready):
        """Print execution summary."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("TARGET SETUP SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Catalog: {target_catalog}")
        logger.info(f"Model: {model_name} (version {model_version})")

        if deploy_serving:
            if not endpoint_name:
                model_short = model_name.split('.')[-1]
                endpoint_name = f"target-{make_dns_compliant(model_short)}-endpoint"
            logger.info(f"Serving Endpoint: {endpoint_name}")
            logger.info(f"  URL: {self.host}/serving-endpoints/{endpoint_name}/invocations")

        if create_online_table:
            status = "READY" if online_tables_ready else "NOT READY"
            logger.info(f"Online Tables: {status}")
            if online_store_name:
                logger.info(f"  Online Store: {online_store_name}")
            logger.info(f"  Location: {online_table_target_catalog}.{online_table_target_schema}")

        logger.info("=" * 60)
        logger.info("Target setup complete.")
