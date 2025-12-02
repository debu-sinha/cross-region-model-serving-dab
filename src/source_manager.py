from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sharing, catalog
from databricks.feature_engineering import FeatureEngineeringClient
import mlflow
from utils import setup_logger

logger = setup_logger(__name__)


class SourceManager:
    """
    Manages sharing of ML models and their feature table dependencies via Delta Sharing.
    
    This class handles:
    1. Dynamic recipient creation using target workspace credentials
    2. Discovery of feature table dependencies from model metadata
    3. Creating/updating Delta Shares with models and feature tables
    4. Granting access to recipients
    """
    
    def __init__(self):
        self.w = WorkspaceClient()
        self.fe = FeatureEngineeringClient()
        # Set MLflow to use Unity Catalog
        mlflow.set_registry_uri("databricks-uc")
        self.mlflow_client = mlflow.MlflowClient()

    def _resolve_secret(self, value):
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

    def run(self, model_name, share_name, recipient_name, target_host=None, target_token=None, create_recipient=True):
        """
        Set up Delta Sharing for a model and its dependencies.

        Args:
            model_name: Full model name (catalog.schema.model)
            share_name: Name of the Delta Share to create/update
            recipient_name: Name of the recipient to grant access to
            target_host: Target workspace URL (for Databricks-to-Databricks sharing)
            target_token: Target workspace token (for Databricks-to-Databricks sharing)
            create_recipient: Whether to create the recipient if it doesn't exist
        """
        logger.info(f"Starting Source Manager for model: {model_name}")

        # Resolve any secret references
        target_host = self._resolve_secret(target_host)
        target_token = self._resolve_secret(target_token)

        # 0. Dynamic Recipient Creation (if credentials provided)
        # The actual recipient name may differ (e.g., 'self' for same-metastore sharing)
        actual_recipient_name = recipient_name
        if target_host and target_token:
            actual_recipient_name = self._setup_databricks_recipient(recipient_name, target_host, target_token, create_recipient)
            if actual_recipient_name != recipient_name:
                logger.info(f"Using recipient '{actual_recipient_name}' instead of requested '{recipient_name}'")
        else:
            # Ensure recipient exists (or create if allowed)
            self._ensure_recipient_exists(recipient_name, create_if_missing=create_recipient)

        # 1. Get Model Details and Dependencies
        logger.info(f"Inspecting model {model_name} for feature dependencies...")
        
        # Get latest model version
        latest_version = self._get_latest_model_version(model_name)
        logger.info(f"Found latest version: {latest_version.version}")

        # Detect Feature Tables from Unity Catalog metadata
        feature_tables = self._extract_feature_tables(latest_version.run_id, model_name)
        
        # 2. Create/Update Share
        self._ensure_share_exists(share_name)

        # 3. Add Model and Dependencies to Share
        self._add_objects_to_share(share_name, model_name, feature_tables)

        # 4. Grant Access to Recipient
        self._grant_recipient_access(share_name, actual_recipient_name, target_host, create_recipient)

        logger.info("Source setup complete.")

        return {
            "model": model_name,
            "version": latest_version.version,
            "share": share_name,
            "recipient": actual_recipient_name,
            "feature_tables": feature_tables
        }

    def _setup_databricks_recipient(self, recipient_name, target_host, target_token, create_if_missing=True):
        """
        Create or update a Databricks-to-Databricks recipient.

        If target workspace has the same metastore as source, uses the built-in 'self' recipient
        instead of creating a new one (which would fail with "same sharing identifier" error).

        Returns the actual recipient name to use (may be 'self' if same metastore).
        """
        logger.info(f"Target credentials provided. Target Host: {target_host}")

        try:
            logger.info("Creating WorkspaceClient for target workspace...")
            target_w = WorkspaceClient(host=target_host, token=target_token)

            # Fetch current metastore of the target workspace
            logger.info("Fetching current metastore of target workspace...")
            try:
                # Use summary() instead of current() to get full metastore info including global_metastore_id
                # current() returns MetastoreAssignment which doesn't have global_metastore_id
                target_metastore = target_w.metastores.summary()
                target_global_id = target_metastore.global_metastore_id
                logger.info(f"Retrieved Target Global Metastore ID: {target_global_id}")
            except TimeoutError as te:
                logger.error(f"Timeout while fetching metastore from target workspace: {te}")
                logger.error(f"This usually means the target workspace URL is incorrect or unreachable: {target_host}")
                raise
            except Exception as me:
                logger.error(f"Error fetching metastore from target workspace: {me}")
                logger.error("Check if the token has permissions to access metastore API")
                raise

            # Check if source and target are the same metastore
            source_metastore = self.w.metastores.summary()
            source_global_id = source_metastore.global_metastore_id
            logger.info(f"Source Global Metastore ID: {source_global_id}")

            if source_global_id == target_global_id:
                # Same metastore - use the built-in 'self' recipient
                logger.info("Source and target are the same metastore. Using built-in 'self' recipient.")
                logger.info(f"Note: Requested recipient '{recipient_name}' will be ignored in favor of 'self'.")
                # Return 'self' to indicate we should use the self recipient
                return "self"
            else:
                # Different metastores - create/update the D2D recipient
                logger.info("Source and target are different metastores. Creating D2D recipient...")
                self._ensure_recipient_exists(recipient_name, target_global_id, create_if_missing)
                return recipient_name

        except Exception as e:
            logger.error(f"Failed to dynamically configure recipient: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            raise

    def _ensure_recipient_exists(self, recipient_name, global_metastore_id=None, create_if_missing=True):
        """Ensure recipient exists, create if not."""
        try:
            logger.info(f"Checking if recipient {recipient_name} exists...")
            existing = self.w.recipients.get(recipient_name)
            
            if global_metastore_id:
                # Check if existing recipient is D2D compatible
                if existing.authentication_type == sharing.AuthenticationType.TOKEN:
                    logger.warning(f"Recipient {recipient_name} exists but is TOKEN-based, not DATABRICKS.")
                    logger.warning(f"Will use existing recipient as-is. D2D sharing may not work correctly.")
                    logger.warning(f"Consider manually deleting recipient '{recipient_name}' if you want to switch to D2D.")
                else:
                    logger.info(f"Recipient {recipient_name} exists with DATABRICKS auth. Updating metastore ID...")
                    self.w.recipients.update(
                        name=recipient_name,
                        new_name=recipient_name,  # Keep same name
                        comment=f"Updated with metastore ID: {global_metastore_id}",
                        data_recipient_global_metastore_id=global_metastore_id
                    )
                    logger.info(f"Updated recipient {recipient_name}")
            else:
                logger.info(f"Recipient {recipient_name} already exists.")
                
        except Exception as re:
            error_str = str(re).lower()
            if "does not exist" in error_str or "not_found" in error_str or "notfound" in error_str:
                if not create_if_missing:
                    raise ValueError(f"Recipient {recipient_name} does not exist and create_recipient is False")
                
                logger.info(f"Creating new recipient {recipient_name}...")
                
                if global_metastore_id:
                    # Databricks-to-Databricks sharing
                    self.w.recipients.create(
                        name=recipient_name,
                        authentication_type=sharing.AuthenticationType.DATABRICKS,
                        data_recipient_global_metastore_id=global_metastore_id
                    )
                else:
                    # Token-based sharing (for external recipients)
                    rec = self.w.recipients.create(
                        name=recipient_name,
                        authentication_type=sharing.AuthenticationType.TOKEN
                    )
                    logger.info(f"Created recipient {recipient_name}")
                    
                    if rec.tokens:
                        activation_link = rec.tokens[0].activation_url
                        logger.warning(f"\n{'='*80}")
                        logger.warning(f"ACTION REQUIRED: Recipient {recipient_name} created with TOKEN authentication.")
                        logger.warning(f"You MUST manually activate this recipient to get the sharing token.")
                        logger.warning(f"Activation Link: {activation_link}")
                        logger.warning(f"{'='*80}\n")
                    else:
                        logger.warning(f"Could not retrieve activation link for recipient {recipient_name}")
            else:
                logger.error(f"Error with recipient operations: {re}")
                raise

    def _get_latest_model_version(self, model_name):
        """Get the latest version of a model from Unity Catalog."""
        versions = self.mlflow_client.search_model_versions(f"name='{model_name}'")
        if not versions:
            raise ValueError(f"No versions found for model {model_name}")
        
        latest_version = sorted(versions, key=lambda x: int(x.version), reverse=True)[0]
        return latest_version

    def _extract_feature_tables(self, run_id, model_name=None):
        """
        Extract feature table dependencies using Databricks SDK.
        This reads the dependencies directly from the model version metadata.
        """
        feature_tables = []
        
        if not model_name:
            logger.warning("model_name not provided to _extract_feature_tables. Cannot fetch dependencies.")
            return feature_tables

        try:
            # Get version from run_id
            client = mlflow.MlflowClient()
            versions = client.search_model_versions(f"name='{model_name}'")
            target_version = None
            for v in versions:
                if v.run_id == run_id:
                    target_version = v.version
                    break
            
            if not target_version:
                logger.warning(f"Could not find model version for run_id {run_id}")
                return feature_tables

            logger.info(f"Fetching dependencies for {model_name} version {target_version}...")
            
            # Get model version details including dependencies
            # Note: w.model_versions maps to the correct API for this
            mv = self.w.model_versions.get(
                full_name=model_name, 
                version=target_version
            )
            
            # Parse dependencies from model_version_dependencies
            if mv.model_version_dependencies and mv.model_version_dependencies.dependencies:
                for dep in mv.model_version_dependencies.dependencies:
                    # Each Dependency can have: connection, credential, function, or table
                    if dep.table and dep.table.table_full_name:
                        table_name = dep.table.table_full_name
                        if table_name not in feature_tables:
                            feature_tables.append(table_name)
                            logger.info(f"Detected feature table dependency: {table_name}")
            
            if feature_tables:
                logger.info(f"Successfully extracted {len(feature_tables)} feature tables via SDK.")
            else:
                logger.info("No feature table dependencies found in model version metadata.")
                
        except Exception as e:
            logger.warning(f"SDK dependency extraction failed: {e}")
            import traceback
            logger.error(traceback.format_exc())

        return feature_tables

    def _ensure_share_exists(self, share_name):
        """Create share if it doesn't exist."""
        logger.info(f"Creating/Updating Share: {share_name}")
        try:
            self.w.shares.get(share_name)
            logger.info(f"Share {share_name} already exists.")
        except Exception as e:
            error_str = str(e).lower()
            if "not found" in error_str or "does not exist" in error_str:
                self.w.shares.create(name=share_name)
                logger.info(f"Created share {share_name}")
            else:
                raise

    def _add_objects_to_share(self, share_name, model_name, feature_tables):
        """Add model and feature tables to the share."""
        logger.info(f"Adding model and {len(feature_tables)} feature tables to share...")
        
        # Get current share contents to avoid duplicate errors
        try:
            current_share = self.w.shares.get(share_name)
            existing_objects = set()
            if current_share.objects:
                for obj in current_share.objects:
                    existing_objects.add(obj.name)
        except Exception:
            existing_objects = set()
        
        updates = []
        
        # Add Model (if not already in share)
        if model_name not in existing_objects:
            updates.append(sharing.SharedDataObjectUpdate(
                action=sharing.SharedDataObjectUpdateAction.ADD,
                data_object=sharing.SharedDataObject(
                    name=model_name,
                    data_object_type=sharing.SharedDataObjectDataObjectType.MODEL
                )
            ))
            logger.info(f"Will add model: {model_name}")
        else:
            logger.info(f"Model {model_name} already in share")
        
        # Add Feature Tables (if not already in share)
        for ft in feature_tables:
            if ft not in existing_objects:
                updates.append(sharing.SharedDataObjectUpdate(
                    action=sharing.SharedDataObjectUpdateAction.ADD,
                    data_object=sharing.SharedDataObject(
                        name=ft,
                        data_object_type=sharing.SharedDataObjectDataObjectType.TABLE,
                        # Enable history sharing for time-travel queries
                        history_data_sharing_status=sharing.SharedDataObjectHistoryDataSharingStatus.ENABLED
                    )
                ))
                logger.info(f"Will add feature table: {ft}")
            else:
                logger.info(f"Feature table {ft} already in share")
        
        if updates:
            try:
                self.w.shares.update(name=share_name, updates=updates)
                logger.info(f"Added {len(updates)} objects to share.")
            except Exception as e:
                error_str = str(e).lower()
                if "already exists" in error_str:
                    logger.warning(f"Some objects already in share: {e}")
                else:
                    logger.error(f"Failed to update share: {e}")
                    raise
        else:
            logger.info("No new objects to add to share")

    def _grant_recipient_access(self, share_name, recipient_name, target_host=None, create_if_missing=True):
        """Grant SELECT access on share to recipient."""
        logger.info(f"Granting access to recipient: {recipient_name}")
        
        # Ensure recipient exists
        try:
            self.w.recipients.get(recipient_name)
        except Exception as e:
            error_str = str(e).lower()
            if "not found" in error_str or "does not exist" in error_str:
                if not create_if_missing:
                    raise ValueError(f"Recipient {recipient_name} does not exist and create_recipient is False")

                if not target_host:
                    logger.info(f"Recipient {recipient_name} does not exist. Creating TOKEN-based recipient...")
                    self.w.recipients.create(
                        name=recipient_name,
                        authentication_type=sharing.AuthenticationType.TOKEN
                    )
                    logger.info(f"Created TOKEN-based recipient {recipient_name}")
                else:
                    logger.error(f"Recipient {recipient_name} should have been created earlier.")
                    raise
            else:
                raise
        
        # Grant SELECT privilege on share to recipient
        # Use the correct API: shares.update_permissions or grants API
        try:
            # Method 1: Using share permissions update (recommended)
            self.w.shares.update_permissions(
                name=share_name,
                changes=[
                    catalog.PermissionsChange(
                        principal=recipient_name,
                        add=[catalog.Privilege.SELECT]
                    )
                ]
            )
            logger.info(f"Granted SELECT on share {share_name} to recipient {recipient_name}")
        except AttributeError:
            # Method 2: Fallback to grants API if update_permissions not available
            try:
                from databricks.sdk.service.catalog import SecurableType, PermissionsChange, Privilege
                self.w.grants.update(
                    full_name=share_name,
                    securable_type=SecurableType.SHARE,
                    changes=[
                        PermissionsChange(
                            principal=recipient_name,
                            add=[Privilege.SELECT]
                        )
                    ]
                )
                logger.info(f"Granted SELECT on share {share_name} to recipient {recipient_name}")
            except Exception as e2:
                logger.warning(f"Could not grant via grants API: {e2}")
                # Try SQL as last resort
                try:
                    from pyspark.sql import SparkSession
                    spark = SparkSession.builder.getOrCreate()
                    spark.sql(f"GRANT SELECT ON SHARE `{share_name}` TO RECIPIENT `{recipient_name}`")
                    logger.info(f"Granted SELECT via SQL")
                except Exception as e3:
                    logger.warning(f"Could not grant via SQL: {e3}")
        except Exception as e:
            error_str = str(e).lower()
            if "already" in error_str or "exists" in error_str:
                logger.info(f"Recipient {recipient_name} already has access to share {share_name}")
            else:
                logger.warning(f"Failed to grant permissions: {e}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Set up Delta Sharing for a model")
    parser.add_argument("--model_name", required=True, help="Full model name (catalog.schema.model)")
    parser.add_argument("--share_name", required=True, help="Name of the share to create/update")
    parser.add_argument("--recipient_name", required=True, help="Name of the recipient")
    parser.add_argument("--target_host", help="Target workspace URL (for D2D sharing)")
    parser.add_argument("--target_token", help="Target workspace token (for D2D sharing)")
    
    args = parser.parse_args()
    
    manager = SourceManager()
    result = manager.run(
        model_name=args.model_name,
        share_name=args.share_name,
        recipient_name=args.recipient_name,
        target_host=args.target_host,
        target_token=args.target_token
    )
    
    print("\n" + "="*60)
    print("SHARING SETUP COMPLETE")
    print("="*60)
    print(f"Model: {result['model']} (version {result['version']})")
    print(f"Share: {result['share']}")
    print(f"Recipient: {result['recipient']}")
    print(f"Feature Tables: {result['feature_tables']}")
    print("="*60)


if __name__ == "__main__":
    main()