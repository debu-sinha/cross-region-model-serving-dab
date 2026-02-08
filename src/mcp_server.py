"""
MCP server for cross-region model serving on Databricks.

Exposes SourceManager and TargetManager operations as MCP tools,
allowing AI assistants to set up Delta Sharing, deploy serving
endpoints, and inspect model dependencies through natural language.

Run directly:  python src/mcp_server.py
Or via entry point:  cross-region-mcp
"""

import io
import json
import logging
import os
import sys
from contextlib import contextmanager
from typing import Optional

# Ensure sibling modules are importable when running directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("cross-region-model-serving")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _or_none(value: str) -> Optional[str]:
    """Convert empty string to None for optional parameters."""
    return value if value else None


@contextmanager
def capture_logs():
    """Redirect project loggers to a buffer during a tool call.

    MCP stdio transport uses stdout for JSON-RPC, so any logger writing
    to stdout would corrupt the protocol. This context manager swaps
    handlers on the project's loggers so output goes to a StringIO buffer
    instead.
    """
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))

    logger_names = ["source_manager", "target_manager", "utils", "__main__"]
    saved = {}
    for name in logger_names:
        lgr = logging.getLogger(name)
        saved[name] = lgr.handlers[:]
        lgr.handlers = [handler]

    try:
        yield buf
    finally:
        for name in logger_names:
            logging.getLogger(name).handlers = saved[name]


def _get_client(host: str = "", token: str = ""):
    """Create a WorkspaceClient. Uses explicit creds if provided, else env vars."""
    from databricks.sdk import WorkspaceClient

    h = _or_none(host)
    t = _or_none(token)
    if h and t:
        return WorkspaceClient(host=h, token=t)
    return WorkspaceClient()


def _serialize_share(share) -> dict:
    """Extract JSON-safe fields from a ShareInfo SDK object."""
    return {
        "name": getattr(share, "name", None),
        "comment": getattr(share, "comment", None),
        "created_at": str(getattr(share, "created_at", "")) or None,
        "created_by": getattr(share, "created_by", None),
    }


def _serialize_recipient(recipient) -> dict:
    """Extract JSON-safe fields from a RecipientInfo SDK object."""
    return {
        "name": getattr(recipient, "name", None),
        "authentication_type": str(getattr(recipient, "authentication_type", "")),
        "comment": getattr(recipient, "comment", None),
        "created_at": str(getattr(recipient, "created_at", "")) or None,
        "created_by": getattr(recipient, "created_by", None),
    }


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def share_model(
    model_name: str,
    share_name: str,
    recipient_name: str,
    target_host: str = "",
    target_token: str = "",
    create_recipient: bool = True,
    source_host: str = "",
    source_token: str = "",
) -> str:
    """Set up Delta Sharing for a Unity Catalog model and its feature table dependencies.

    Creates or updates a Delta Share, adds the model and any detected feature
    tables, and grants access to the recipient. For cross-region D2D sharing,
    provide target_host and target_token.

    Uses DATABRICKS_HOST/TOKEN env vars for the source workspace by default.
    Provide source_host and source_token to use a different source workspace.

    Args:
        model_name: Full model name (catalog.schema.model).
        share_name: Delta Share name to create or update.
        recipient_name: Recipient name (ignored for same-metastore, uses 'self').
        target_host: Target workspace URL for D2D sharing. Leave empty for token-based.
        target_token: Target workspace PAT for D2D sharing. Leave empty for token-based.
        create_recipient: Auto-create recipient if it does not exist.
        source_host: Source workspace URL. Uses env vars if empty.
        source_token: Source workspace PAT. Uses env vars if empty.
    """
    try:
        from source_manager import SourceManager

        # SourceManager uses WorkspaceClient() which reads env vars.
        # Temporarily override if explicit source creds were provided.
        src_h = _or_none(source_host)
        src_t = _or_none(source_token)
        old_host = os.environ.get("DATABRICKS_HOST")
        old_token = os.environ.get("DATABRICKS_TOKEN")

        if src_h and src_t:
            os.environ["DATABRICKS_HOST"] = src_h
            os.environ["DATABRICKS_TOKEN"] = src_t

        try:
            with capture_logs() as log_buf:
                mgr = SourceManager()
                result = mgr.run(
                    model_name=model_name,
                    share_name=share_name,
                    recipient_name=recipient_name,
                    target_host=_or_none(target_host),
                    target_token=_or_none(target_token),
                    create_recipient=create_recipient,
                )
                logs = log_buf.getvalue()

            return json.dumps({"status": "success", "data": result, "logs": logs})
        finally:
            # Restore original env vars
            if src_h and src_t:
                if old_host is not None:
                    os.environ["DATABRICKS_HOST"] = old_host
                else:
                    os.environ.pop("DATABRICKS_HOST", None)
                if old_token is not None:
                    os.environ["DATABRICKS_TOKEN"] = old_token
                else:
                    os.environ.pop("DATABRICKS_TOKEN", None)

    except Exception as exc:
        return json.dumps({
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
        })


@mcp.tool()
def consume_shared_model(
    target_host: str,
    target_token: str,
    share_name: str,
    provider_name: str,
    target_catalog: str,
    deploy_serving: bool = False,
    serving_endpoint_name: str = "",
    create_online_table: bool = False,
    online_store_name: str = "",
    create_online_store: bool = True,
    online_table_target_catalog: str = "",
    online_table_target_schema: str = "",
    use_existing_catalog: bool = False,
) -> str:
    """Consume a Delta Shared model on a target Databricks workspace.

    Runs a 4-phase pipeline: (1) create catalog from share, (2) discover model
    and feature dependencies, (3) create online tables if needed, (4) deploy
    serving endpoint.

    Args:
        target_host: Target workspace URL.
        target_token: Target workspace personal access token.
        share_name: Delta Share to consume.
        provider_name: Provider name ('self' for same-metastore).
        target_catalog: Catalog to create from the share or existing catalog.
        deploy_serving: Deploy a model serving endpoint.
        serving_endpoint_name: Endpoint name (auto-generated if empty).
        create_online_table: Create online feature tables.
        online_store_name: Online store name (auto-generated if empty).
        create_online_store: Create online store if it does not exist.
        online_table_target_catalog: Writable catalog for online tables.
        online_table_target_schema: Schema for online tables.
        use_existing_catalog: Use existing catalog instead of creating one.
    """
    try:
        from target_manager import TargetManager

        with capture_logs() as log_buf:
            mgr = TargetManager(target_host, target_token)
            mgr.run(
                share_name=share_name,
                provider_name=provider_name,
                target_catalog=target_catalog,
                deploy_serving=deploy_serving,
                serving_endpoint_name=_or_none(serving_endpoint_name),
                create_online_table=create_online_table,
                online_store_name=_or_none(online_store_name),
                create_online_store=create_online_store,
                online_table_target_catalog=_or_none(online_table_target_catalog),
                online_table_target_schema=_or_none(online_table_target_schema),
                use_existing_catalog=use_existing_catalog,
            )
            logs = log_buf.getvalue()

        return json.dumps({"status": "success", "logs": logs})

    except Exception as exc:
        return json.dumps({
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
        })


@mcp.tool()
def inspect_model_dependencies(
    model_name: str,
    version: str = "",
    workspace_host: str = "",
    workspace_token: str = "",
) -> str:
    """Inspect a Unity Catalog model's feature table dependencies.

    Returns feature tables, whether the model uses feature lookups, and
    online table specs needed for serving.

    Uses DATABRICKS_HOST/TOKEN env vars by default. Provide workspace_host
    and workspace_token to connect to a different workspace.

    Args:
        model_name: Full model name (catalog.schema.model).
        version: Model version to inspect (defaults to latest).
        workspace_host: Workspace URL. Uses env vars if empty.
        workspace_token: Workspace PAT. Uses env vars if empty.
    """
    try:
        from utils import extract_model_feature_dependencies

        with capture_logs() as log_buf:
            w = _get_client(workspace_host, workspace_token)
            result = extract_model_feature_dependencies(
                w, model_name, version=_or_none(version)
            )
            logs = log_buf.getvalue()

        return json.dumps({"status": "success", "data": result, "logs": logs})

    except Exception as exc:
        return json.dumps({
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
        })


@mcp.tool()
def list_shares(
    workspace_host: str = "",
    workspace_token: str = "",
) -> str:
    """List all Delta Shares in a Databricks workspace.

    Uses DATABRICKS_HOST/TOKEN env vars by default. Provide workspace_host
    and workspace_token to query a different workspace.

    Args:
        workspace_host: Workspace URL. Uses env vars if empty.
        workspace_token: Workspace PAT. Uses env vars if empty.
    """
    try:
        w = _get_client(workspace_host, workspace_token)
        shares = [_serialize_share(s) for s in w.shares.list()]
        return json.dumps({"status": "success", "data": shares})

    except Exception as exc:
        return json.dumps({
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
        })


@mcp.tool()
def list_recipients(
    workspace_host: str = "",
    workspace_token: str = "",
) -> str:
    """List all Delta Sharing recipients in a Databricks workspace.

    Uses DATABRICKS_HOST/TOKEN env vars by default. Provide workspace_host
    and workspace_token to query a different workspace.

    Args:
        workspace_host: Workspace URL. Uses env vars if empty.
        workspace_token: Workspace PAT. Uses env vars if empty.
    """
    try:
        w = _get_client(workspace_host, workspace_token)
        recipients = [_serialize_recipient(r) for r in w.recipients.list()]
        return json.dumps({"status": "success", "data": recipients})

    except Exception as exc:
        return json.dumps({
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
        })


@mcp.tool()
def get_share_details(
    share_name: str,
    workspace_host: str = "",
    workspace_token: str = "",
) -> str:
    """Get details of a Delta Share including its shared objects and permissions.

    Uses DATABRICKS_HOST/TOKEN env vars by default. Provide workspace_host
    and workspace_token to query a different workspace.

    Args:
        share_name: Name of the Delta Share to inspect.
        workspace_host: Workspace URL. Uses env vars if empty.
        workspace_token: Workspace PAT. Uses env vars if empty.
    """
    try:
        w = _get_client(workspace_host, workspace_token)

        share = w.shares.get(share_name)
        objects = []
        if share.objects:
            for obj in share.objects:
                objects.append({
                    "name": getattr(obj, "name", None),
                    "data_object_type": str(getattr(obj, "data_object_type", "")),
                    "status": str(getattr(obj, "status", "")),
                })

        perms = []
        try:
            perm_info = w.shares.share_permissions(share_name)
            if perm_info and perm_info.privilege_assignments:
                for pa in perm_info.privilege_assignments:
                    perms.append({
                        "principal": getattr(pa, "principal", None),
                        "privileges": [str(p) for p in (pa.privileges or [])],
                    })
        except Exception:
            pass  # permissions may not be accessible

        return json.dumps({
            "status": "success",
            "data": {
                "name": share.name,
                "comment": getattr(share, "comment", None),
                "objects": objects,
                "permissions": perms,
            },
        })

    except Exception as exc:
        return json.dumps({
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
        })


@mcp.tool()
def check_endpoint_status(
    target_host: str,
    target_token: str,
    endpoint_name: str,
) -> str:
    """Check the status of a model serving endpoint on a Databricks workspace.

    Args:
        target_host: Workspace URL.
        target_token: Workspace personal access token.
        endpoint_name: Serving endpoint name.
    """
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(host=target_host, token=target_token)
        ep = w.serving_endpoints.get(endpoint_name)

        state_info = {}
        if ep.state:
            state_info["ready"] = str(getattr(ep.state, "ready", ""))
            state_info["config_update"] = str(getattr(ep.state, "config_update", ""))

        entities = []
        if ep.config and ep.config.served_entities:
            for ent in ep.config.served_entities:
                entities.append({
                    "name": getattr(ent, "entity_name", None) or getattr(ent, "name", None),
                    "version": getattr(ent, "entity_version", None),
                })

        return json.dumps({
            "status": "success",
            "data": {
                "name": ep.name,
                "state": state_info,
                "served_entities": entities,
                "invocation_url": f"{target_host}/serving-endpoints/{endpoint_name}/invocations",
            },
        })

    except Exception as exc:
        return json.dumps({
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
        })


@mcp.tool()
def validate_target_configuration(
    share_name: str,
    provider_name: str,
    target_catalog: str,
    create_online_table: bool = False,
    online_table_target_catalog: str = "",
    online_table_target_schema: str = "",
) -> str:
    """Pre-validate target configuration before running consume_shared_model.

    Checks required parameters and returns specific error messages with
    remediation hints if the configuration is invalid.

    Args:
        share_name: Delta Share name.
        provider_name: Provider name ('self' for same-metastore).
        target_catalog: Target catalog name.
        create_online_table: Whether online tables will be created.
        online_table_target_catalog: Writable catalog for online tables.
        online_table_target_schema: Schema for online tables.
    """
    try:
        from utils import validate_target_config

        validate_target_config(
            share_name=share_name,
            provider_name=provider_name,
            target_catalog=target_catalog,
            create_online_table=create_online_table,
            online_table_target_catalog=_or_none(online_table_target_catalog) or "",
            online_table_target_schema=_or_none(online_table_target_schema) or "",
        )
        return json.dumps({"status": "valid", "message": "Configuration is valid."})

    except Exception as exc:
        return json.dumps({
            "status": "invalid",
            "error_type": type(exc).__name__,
            "message": str(exc),
        })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    # Redirect root logger to stderr so it doesn't interfere with stdio transport
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING, force=True)
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
