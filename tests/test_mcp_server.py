import json
import sys
import unittest
from unittest.mock import MagicMock, patch


# Mock databricks.feature_engineering before any source_manager import,
# since the module isn't installed locally (only in CI / on cluster).
if "databricks.feature_engineering" not in sys.modules:
    _mock_fe_mod = MagicMock()
    sys.modules["databricks.feature_engineering"] = _mock_fe_mod


class TestShareModel(unittest.TestCase):
    @patch("source_manager.mlflow")
    @patch("source_manager.FeatureEngineeringClient")
    @patch("source_manager.WorkspaceClient")
    def test_share_model_success(self, mock_wc, mock_fe, mock_mlflow):
        """share_model returns success JSON when SourceManager.run() succeeds."""
        from mcp_server import share_model

        mock_w = mock_wc.return_value
        mock_w.shares.get.side_effect = Exception("Share does not exist")
        mock_w.recipients.get.return_value = MagicMock()

        mock_client = MagicMock()
        mock_mlflow.MlflowClient.return_value = mock_client
        mock_version = MagicMock()
        mock_version.version = "1"
        mock_version.run_id = "run-123"
        mock_client.search_model_versions.return_value = [mock_version]

        result = json.loads(share_model("cat.sch.model", "my_share", "my_recip"))
        self.assertEqual(result["status"], "success")
        self.assertIn("data", result)

    @patch("source_manager.mlflow")
    @patch("source_manager.FeatureEngineeringClient")
    @patch("source_manager.WorkspaceClient")
    def test_share_model_error(self, mock_wc, mock_fe, mock_mlflow):
        """share_model returns error JSON when SourceManager.run() raises."""
        from mcp_server import share_model

        mock_wc.side_effect = Exception("Connection refused")

        result = json.loads(share_model("bad.model", "s", "r"))
        self.assertEqual(result["status"], "error")
        self.assertIn("message", result)
        self.assertIn("error_type", result)


class TestConsumeSharedModel(unittest.TestCase):
    @patch("target_manager.WorkspaceClient")
    def test_consume_shared_model_success(self, mock_wc):
        """consume_shared_model returns success JSON with captured logs."""
        from mcp_server import consume_shared_model

        mock_w = mock_wc.return_value
        mock_w.catalogs.get.side_effect = Exception("Catalog does not exist")
        mock_w.serving_endpoints.get.side_effect = Exception("Not found")

        mock_schema = MagicMock()
        mock_schema.name = "default"
        mock_w.schemas.list.return_value = [mock_schema]

        mock_model = MagicMock()
        mock_model.full_name = "shared_cat.default.my_model"
        mock_w.registered_models.list.return_value = [mock_model]

        result = json.loads(consume_shared_model(
            target_host="https://host.databricks.com",
            target_token="dapi-xxx",
            share_name="my_share",
            provider_name="self",
            target_catalog="shared_cat",
            deploy_serving=False,
        ))
        self.assertEqual(result["status"], "success")
        self.assertIn("logs", result)

    def test_consume_shared_model_error(self):
        """consume_shared_model returns error JSON on failure."""
        from mcp_server import consume_shared_model

        # Empty host/token should cause WorkspaceClient to fail
        with patch("target_manager.WorkspaceClient", side_effect=Exception("Bad creds")):
            result = json.loads(consume_shared_model(
                target_host="",
                target_token="",
                share_name="s",
                provider_name="p",
                target_catalog="c",
            ))
        self.assertEqual(result["status"], "error")
        self.assertIn("message", result)


class TestInspectModelDependencies(unittest.TestCase):
    @patch("mlflow.MlflowClient")
    @patch("mlflow.set_registry_uri")
    @patch("databricks.sdk.WorkspaceClient")
    def test_inspect_model_deps(self, mock_wc, mock_set_uri, mock_mlflow_client):
        """inspect_model_dependencies returns result from extract function."""
        from mcp_server import inspect_model_dependencies

        mock_w = mock_wc.return_value
        mock_mv = MagicMock()
        mock_mv.model_version_dependencies = None
        mock_w.model_versions.get.return_value = mock_mv

        mock_client = mock_mlflow_client.return_value
        mock_version = MagicMock()
        mock_version.version = "1"
        mock_version.run_id = "run-1"
        mock_client.search_model_versions.return_value = [mock_version]

        result = json.loads(inspect_model_dependencies("cat.sch.model"))
        self.assertEqual(result["status"], "success")
        self.assertIn("data", result)


class TestListShares(unittest.TestCase):
    @patch("databricks.sdk.WorkspaceClient")
    def test_list_shares_success(self, mock_wc_cls):
        """list_shares returns JSON list of share objects."""
        from mcp_server import list_shares

        mock_share = MagicMock()
        mock_share.name = "test_share"
        mock_share.comment = "A test share"
        mock_share.created_at = 1700000000
        mock_share.created_by = "user@example.com"

        mock_w = mock_wc_cls.return_value
        mock_w.shares.list.return_value = [mock_share]

        result = json.loads(list_shares())
        self.assertEqual(result["status"], "success")
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["name"], "test_share")

    @patch("databricks.sdk.WorkspaceClient")
    def test_list_shares_error(self, mock_wc_cls):
        """list_shares returns error when workspace is unavailable."""
        from mcp_server import list_shares

        mock_wc_cls.side_effect = Exception("No auth")
        result = json.loads(list_shares())
        self.assertEqual(result["status"], "error")
        self.assertIn("No auth", result["message"])


class TestListRecipients(unittest.TestCase):
    @patch("databricks.sdk.WorkspaceClient")
    def test_list_recipients_error(self, mock_wc_cls):
        """list_recipients returns error when workspace is unavailable."""
        from mcp_server import list_recipients

        mock_wc_cls.side_effect = Exception("No auth")
        result = json.loads(list_recipients())
        self.assertEqual(result["status"], "error")


class TestGetShareDetails(unittest.TestCase):
    @patch("databricks.sdk.WorkspaceClient")
    def test_get_share_details_not_found(self, mock_wc_cls):
        """get_share_details returns error for non-existent share."""
        from mcp_server import get_share_details

        mock_w = mock_wc_cls.return_value
        mock_w.shares.get.side_effect = Exception("Share not found")

        result = json.loads(get_share_details("missing_share"))
        self.assertEqual(result["status"], "error")
        self.assertIn("not found", result["message"])

    @patch("databricks.sdk.WorkspaceClient")
    def test_get_share_details_success(self, mock_wc_cls):
        """get_share_details returns share objects and permissions."""
        from mcp_server import get_share_details

        mock_obj = MagicMock()
        mock_obj.name = "cat.sch.my_model"
        mock_obj.data_object_type = "MODEL"
        mock_obj.status = "ACTIVE"

        mock_share = MagicMock()
        mock_share.name = "my_share"
        mock_share.comment = None
        mock_share.objects = [mock_obj]

        mock_pa = MagicMock()
        mock_pa.principal = "my_recip"
        mock_pa.privileges = ["SELECT"]
        mock_perm = MagicMock()
        mock_perm.privilege_assignments = [mock_pa]

        mock_w = mock_wc_cls.return_value
        mock_w.shares.get.return_value = mock_share
        mock_w.shares.share_permissions.return_value = mock_perm

        result = json.loads(get_share_details("my_share"))
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["data"]["name"], "my_share")
        self.assertEqual(len(result["data"]["objects"]), 1)


class TestCheckEndpointStatus(unittest.TestCase):
    @patch("databricks.sdk.WorkspaceClient")
    def test_check_endpoint_success(self, mock_wc_cls):
        """check_endpoint_status returns endpoint state."""
        from mcp_server import check_endpoint_status

        mock_ep = MagicMock()
        mock_ep.name = "my-endpoint"
        mock_ep.state.ready = "READY"
        mock_ep.state.config_update = "NOT_UPDATING"
        mock_ep.config.served_entities = []

        mock_w = mock_wc_cls.return_value
        mock_w.serving_endpoints.get.return_value = mock_ep

        result = json.loads(check_endpoint_status(
            "https://host.databricks.com", "dapi-xxx", "my-endpoint"
        ))
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["data"]["name"], "my-endpoint")
        self.assertEqual(result["data"]["state"]["ready"], "READY")

    @patch("databricks.sdk.WorkspaceClient")
    def test_check_endpoint_not_found(self, mock_wc_cls):
        """check_endpoint_status returns error for missing endpoint."""
        from mcp_server import check_endpoint_status

        mock_w = mock_wc_cls.return_value
        mock_w.serving_endpoints.get.side_effect = Exception("Endpoint does not exist")

        result = json.loads(check_endpoint_status(
            "https://host.databricks.com", "dapi-xxx", "missing"
        ))
        self.assertEqual(result["status"], "error")


class TestValidateTargetConfiguration(unittest.TestCase):
    def test_valid_config(self):
        """validate_target_configuration returns valid for good config."""
        from mcp_server import validate_target_configuration

        result = json.loads(validate_target_configuration(
            share_name="my_share",
            provider_name="self",
            target_catalog="shared_cat",
            create_online_table=False,
        ))
        self.assertEqual(result["status"], "valid")

    def test_invalid_config_missing_online_table_catalog(self):
        """validate_target_configuration catches missing online table catalog."""
        from mcp_server import validate_target_configuration

        result = json.loads(validate_target_configuration(
            share_name="my_share",
            provider_name="self",
            target_catalog="shared_cat",
            create_online_table=True,
            online_table_target_catalog="",
            online_table_target_schema="",
        ))
        self.assertEqual(result["status"], "invalid")
        self.assertIn("online_table_target_catalog", result["message"])


if __name__ == "__main__":
    unittest.main()
