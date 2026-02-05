import logging
import sys
import os
import re
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any

def setup_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

logger = setup_logger(__name__)


# =============================================================================
# DNS/NAMING UTILITIES
# =============================================================================

def make_dns_compliant(name: str) -> str:
    """
    Convert a name to DNS-compliant format for online stores.
    - Replace underscores with hyphens
    - Remove any non-alphanumeric characters except hyphens
    - Convert to lowercase
    - Ensure it doesn't start or end with a hyphen
    - Collapse multiple hyphens
    """
    name = name.replace('_', '-')
    name = re.sub(r'[^a-zA-Z0-9-]', '', name)
    name = name.lower()
    name = name.strip('-')
    name = re.sub(r'-+', '-', name)
    return name


# =============================================================================
# RUNTIME DETECTION
# =============================================================================

def is_databricks_runtime() -> bool:
    """Check if running inside Databricks Runtime (cluster)."""
    return 'DATABRICKS_RUNTIME_VERSION' in os.environ


def get_feature_engineering_client():
    """
    Get FeatureEngineeringClient if available.
    Returns None if not in Databricks Runtime.
    """
    if not is_databricks_runtime():
        logger.warning("FeatureEngineeringClient requires Databricks Runtime.")
        logger.warning("Online table operations will be skipped when running locally.")
        return None

    try:
        from databricks.feature_engineering import FeatureEngineeringClient
        return FeatureEngineeringClient()
    except ImportError:
        logger.warning("databricks-feature-engineering not installed.")
        return None


# =============================================================================
# WAIT/POLLING UTILITIES
# =============================================================================

def wait_for_online_store_available(fe, store_name: str, timeout_seconds: int = 900, poll_interval: int = 30):
    """
    Wait for online store to become AVAILABLE.

    Args:
        fe: FeatureEngineeringClient instance
        store_name: Name of the online store
        timeout_seconds: Maximum time to wait (default 15 minutes)
        poll_interval: Time between status checks (default 30 seconds)

    Returns:
        Online store object when available

    Raises:
        TimeoutError: If store doesn't become available within timeout
    """
    logger.info(f"Waiting for online store '{store_name}' to become AVAILABLE...")
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        try:
            store = fe.get_online_store(name=store_name)
            state_str = str(store.state).upper()

            if "AVAILABLE" in state_str:
                logger.info(f"Online store '{store_name}' is now AVAILABLE")
                return store

            elapsed = int(time.time() - start_time)
            logger.info(f"[{elapsed}s] Online store state: {store.state}. Waiting...")

        except Exception as e:
            logger.warning(f"Error checking store status: {e}")

        time.sleep(poll_interval)

    raise TimeoutError(
        f"Online store '{store_name}' did not become AVAILABLE within {timeout_seconds} seconds. "
        f"Check the Databricks UI for status."
    )


def wait_for_online_table_ready(w, online_table_name: str, timeout_seconds: int = 600, poll_interval: int = 30):
    """
    Wait for online table to reach ONLINE state.

    This is critical - endpoint deployment will fail if online tables aren't ready.

    Args:
        w: WorkspaceClient instance
        online_table_name: Full name of the online table (catalog.schema.table_online)
        timeout_seconds: Maximum time to wait (default 10 minutes)
        poll_interval: Time between status checks (default 30 seconds)

    Returns:
        True if table is ready, False otherwise
    """
    logger.info(f"Waiting for online table '{online_table_name}' to be ONLINE...")
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        try:
            # Use online_tables API to check status
            online_table = w.online_tables.get(name=online_table_name)
            status = online_table.status

            if status:
                state = str(status.detailed_state).upper() if status.detailed_state else ""

                if "ONLINE" in state and "OFFLINE" not in state:
                    logger.info(f"Online table '{online_table_name}' is ONLINE")
                    return True

                elapsed = int(time.time() - start_time)
                message = status.message if status.message else "No message"
                logger.info(f"[{elapsed}s] Online table state: {state}. {message}")

                # Check for terminal failure states
                if "FAILED" in state or "OFFLINE_FAILED" in state:
                    logger.error(f"Online table failed: {message}")
                    return False

        except Exception as e:
            error_str = str(e).lower()
            if "not found" in error_str:
                elapsed = int(time.time() - start_time)
                logger.info(f"[{elapsed}s] Online table not yet visible. Waiting...")
            else:
                logger.warning(f"Error checking online table status: {e}")

        time.sleep(poll_interval)

    logger.warning(f"Timeout waiting for online table '{online_table_name}' after {timeout_seconds}s")
    return False


def wait_for_endpoint_ready(w, endpoint_name: str, timeout_seconds: int = 300, poll_interval: int = 15):
    """
    Wait for serving endpoint to be ready for updates.

    Args:
        w: WorkspaceClient instance
        endpoint_name: Name of the serving endpoint
        timeout_seconds: Maximum time to wait (default 5 minutes)
        poll_interval: Time between status checks (default 15 seconds)

    Returns:
        Endpoint object when ready, None if timeout
    """
    logger.info(f"Waiting for endpoint '{endpoint_name}' to be ready...")
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        try:
            endpoint = w.serving_endpoints.get(endpoint_name)
            state = endpoint.state

            if state:
                ready_str = str(state.ready).upper()
                config_update_str = str(state.config_update).upper()

                elapsed = int(time.time() - start_time)
                logger.info(f"[{elapsed}s] Endpoint state - ready: {ready_str}, config: {config_update_str}")

                if "READY" in ready_str and "IN_PROGRESS" not in config_update_str:
                    logger.info(f"Endpoint '{endpoint_name}' is ready")
                    return endpoint
                elif "NOT_UPDATING" in config_update_str:
                    logger.info(f"Endpoint '{endpoint_name}' is ready (not updating)")
                    return endpoint

        except Exception as e:
            logger.warning(f"Error checking endpoint status: {e}")

        time.sleep(poll_interval)

    logger.warning(f"Timeout waiting for endpoint '{endpoint_name}' to be ready")
    return None


# =============================================================================
# MODEL METADATA UTILITIES
# =============================================================================

def extract_model_feature_dependencies(w, model_name: str, version: str = None) -> Dict[str, Any]:
    """
    Extract feature table dependencies and online table requirements from model metadata.

    Args:
        w: WorkspaceClient instance
        model_name: Full model name (catalog.schema.model)
        version: Optional specific version (defaults to latest)

    Returns:
        Dict with:
            - feature_tables: List of feature table names
            - has_feature_lookups: Boolean indicating if model uses feature lookups
            - online_table_specs: List of dicts with online table requirements
    """
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    client = mlflow.MlflowClient()

    result = {
        "feature_tables": [],
        "has_feature_lookups": False,
        "online_table_specs": [],
        "model_name": model_name,
        "version": version
    }

    try:
        # Get version
        if not version:
            versions = client.search_model_versions(f"name='{model_name}'")
            if versions:
                latest = sorted(versions, key=lambda x: int(x.version), reverse=True)[0]
                version = latest.version
                result["version"] = version

        # Get model version details with dependencies
        mv = w.model_versions.get(full_name=model_name, version=version)

        if mv.model_version_dependencies and mv.model_version_dependencies.dependencies:
            for dep in mv.model_version_dependencies.dependencies:
                if dep.table and dep.table.table_full_name:
                    table_name = dep.table.table_full_name
                    if table_name not in result["feature_tables"]:
                        result["feature_tables"].append(table_name)
                        result["has_feature_lookups"] = True

                        # Extract primary key info if available
                        try:
                            table_info = w.tables.get(table_name)
                            if table_info.columns:
                                # Look for primary key constraint info
                                for col in table_info.columns:
                                    # This is a simplification - real PK detection may vary
                                    pass

                            result["online_table_specs"].append({
                                "source_table": table_name,
                                "table_name_short": table_name.split(".")[-1]
                            })
                        except Exception:
                            result["online_table_specs"].append({
                                "source_table": table_name,
                                "table_name_short": table_name.split(".")[-1]
                            })

        logger.info(f"Model '{model_name}' v{version}: {len(result['feature_tables'])} feature table(s)")
        if result["has_feature_lookups"]:
            logger.info(f"  Feature tables: {result['feature_tables']}")
            logger.info("  Model uses Feature Lookups - online tables required for serving")

    except Exception as e:
        logger.warning(f"Could not extract model dependencies: {e}")

    return result


# =============================================================================
# VALIDATION UTILITIES
# =============================================================================

class ValidationError(Exception):
    """Custom exception for validation errors with remediation hints."""
    def __init__(self, message: str, remediation: str = None):
        self.message = message
        self.remediation = remediation
        super().__init__(self.format_message())

    def format_message(self):
        msg = f"Validation Error: {self.message}"
        if self.remediation:
            msg += f"\n  Remediation: {self.remediation}"
        return msg


def validate_target_config(
    share_name: str,
    provider_name: str,
    target_catalog: str,
    create_online_table: bool,
    online_table_target_catalog: str,
    online_table_target_schema: str
):
    """
    Validate target configuration before execution.

    Raises:
        ValidationError: If configuration is invalid
    """
    errors = []

    if not share_name:
        errors.append(ValidationError(
            "share_name is required",
            "Provide the name of the Delta Share to consume"
        ))

    if not provider_name:
        errors.append(ValidationError(
            "provider_name is required",
            "Use 'self' for same-metastore sharing or the provider name for cross-metastore"
        ))

    if not target_catalog:
        errors.append(ValidationError(
            "target_catalog is required",
            "Provide the catalog name to create from the share"
        ))

    if create_online_table:
        if not online_table_target_catalog:
            errors.append(ValidationError(
                "online_table_target_catalog is required when create_online_table=true",
                "Shared catalogs are read-only. Specify a writable catalog for online tables (e.g., 'main')"
            ))
        if not online_table_target_schema:
            errors.append(ValidationError(
                "online_table_target_schema is required when create_online_table=true",
                "Specify the schema within online_table_target_catalog (e.g., 'default')"
            ))

    if errors:
        error_messages = "\n".join([e.format_message() for e in errors])
        raise ValidationError(f"Configuration validation failed:\n{error_messages}")


# =============================================================================
# APP CONFIGURATION
# =============================================================================

@dataclass
class AppConfig:
    mode: str
    # Source Args
    model_name: Optional[str] = None
    share_name: Optional[str] = None
    recipient_name: Optional[str] = None

    # Target Args
    provider_name: Optional[str] = None
    target_catalog: Optional[str] = None
    target_host: Optional[str] = None
    target_token: Optional[str] = None
    deploy_serving: bool = False
    serving_endpoint_name: Optional[str] = None
    create_online_table: bool = False

    @classmethod
    def from_args(cls, args):
        return cls(
            mode=args.mode,
            model_name=args.model_name,
            share_name=args.share_name,
            recipient_name=args.recipient_name,
            provider_name=args.provider_name,
            target_catalog=args.target_catalog,
            target_host=args.target_host,
            target_token=args.target_token,
            deploy_serving=str(args.deploy_serving).lower() == 'true',
            serving_endpoint_name=args.serving_endpoint_name,
            create_online_table=str(args.create_online_table).lower() == 'true'
        )

    def validate(self):
        if self.mode == "source":
            if not all([self.model_name, self.share_name, self.recipient_name]):
                raise ValueError("Missing required arguments for source mode: model_name, share_name, recipient_name")
        elif self.mode == "target":
            if not all([self.share_name, self.provider_name, self.target_catalog, self.target_host, self.target_token]):
                raise ValueError("Missing required arguments for target mode: share_name, provider_name, target_catalog, target_host, target_token")
