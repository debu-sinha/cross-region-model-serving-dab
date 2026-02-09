import unittest
from unittest.mock import MagicMock, patch
from databricks.sdk.service import sharing


class TestSourceManager(unittest.TestCase):
    @patch('source_manager.WorkspaceClient')
    @patch('source_manager.FeatureEngineeringClient')
    @patch('source_manager.mlflow')
    def test_run_source(self, mock_mlflow, mock_fe, mock_wc):
        """Test basic source sharing flow: create share, add model, grant access."""
        from source_manager import SourceManager

        mock_w = mock_wc.return_value
        mock_w.shares.get.side_effect = Exception("Share does not exist")

        mock_client = MagicMock()
        mock_mlflow.MlflowClient.return_value = mock_client
        mock_version = MagicMock()
        mock_version.version = "1"
        mock_version.run_id = "run-123"
        mock_client.search_model_versions.return_value = [mock_version]

        manager = SourceManager()
        manager.run("catalog.schema.my_model", "my_share", "my_recipient")

        mock_w.shares.create.assert_called_with(name="my_share")
        mock_w.shares.update.assert_called()

    @patch('source_manager.WorkspaceClient')
    @patch('source_manager.FeatureEngineeringClient')
    @patch('source_manager.mlflow')
    def test_run_source_dynamic_recipient(self, mock_mlflow, mock_fe, mock_wc):
        """Test cross-metastore D2D recipient creation with different metastore IDs."""
        from source_manager import SourceManager

        mock_local_w = MagicMock()
        mock_target_w = MagicMock()
        mock_wc.side_effect = [mock_local_w, mock_target_w]

        # Target metastore
        mock_target_metastore = MagicMock()
        mock_target_metastore.global_metastore_id = "aws:us-east-1:target-metastore-uuid"
        mock_target_w.metastores.summary.return_value = mock_target_metastore

        # Source metastore (different from target)
        mock_source_metastore = MagicMock()
        mock_source_metastore.global_metastore_id = "aws:us-west-2:source-metastore-uuid"
        mock_local_w.metastores.summary.return_value = mock_source_metastore

        # First recipients.get raises (in _ensure_recipient_exists), subsequent calls succeed
        # (in _grant_recipient_access, the recipient exists after creation)
        mock_local_w.shares.get.side_effect = Exception("Share does not exist")
        mock_local_w.recipients.get.side_effect = [
            Exception("Recipient does not exist"),  # _ensure_recipient_exists
            MagicMock(),                             # _grant_recipient_access
        ]

        mock_client = MagicMock()
        mock_mlflow.MlflowClient.return_value = mock_client
        mock_version = MagicMock()
        mock_version.version = "1"
        mock_version.run_id = "run-456"
        mock_client.search_model_versions.return_value = [mock_version]

        manager = SourceManager()
        manager.run(
            "catalog.schema.my_model",
            "my_share",
            "my_recipient",
            target_host="https://target.cloud.databricks.com",
            target_token="dapi-target-token",
        )

        mock_target_w.metastores.summary.assert_called()
        mock_local_w.recipients.create.assert_called()
        call_kwargs = mock_local_w.recipients.create.call_args
        self.assertEqual(
            call_kwargs.kwargs.get("authentication_type")
            or call_kwargs[1].get("authentication_type"),
            sharing.AuthenticationType.DATABRICKS,
        )


class TestSourceManagerFunctionDeps(unittest.TestCase):
    @patch('source_manager.WorkspaceClient')
    @patch('source_manager.FeatureEngineeringClient')
    @patch('source_manager.mlflow')
    def test_run_shares_functions(self, mock_mlflow, mock_fe, mock_wc):
        """Functions found in model_version_dependencies are added to the share."""
        from source_manager import SourceManager

        mock_w = mock_wc.return_value
        mock_w.shares.get.side_effect = Exception("Share does not exist")
        mock_w.recipients.get.return_value = MagicMock()

        mock_client = MagicMock()
        mock_mlflow.MlflowClient.return_value = mock_client
        mock_version = MagicMock()
        mock_version.version = "1"
        mock_version.run_id = "run-789"
        mock_client.search_model_versions.return_value = [mock_version]

        # Set up model version with both table and function dependencies
        mock_table_dep = MagicMock()
        mock_table_dep.table.table_full_name = "cat.sch.features"
        mock_table_dep.function = None

        mock_func_dep = MagicMock()
        mock_func_dep.table = None
        mock_func_dep.function.function_full_name = "cat.sch.my_udf"

        mock_mv = MagicMock()
        mock_mv.model_version_dependencies.dependencies = [mock_table_dep, mock_func_dep]
        mock_w.model_versions.get.return_value = mock_mv

        manager = SourceManager()
        result = manager.run("cat.sch.model", "my_share", "my_recipient")

        self.assertIn("functions", result)
        self.assertEqual(result["functions"], ["cat.sch.my_udf"])
        self.assertEqual(result["feature_tables"], ["cat.sch.features"])

        # Verify that shares.update was called and included a FUNCTION object
        update_call = mock_w.shares.update.call_args
        updates = update_call.kwargs.get("updates") or update_call[1].get("updates")
        object_types = [str(u.data_object.data_object_type) for u in updates]
        self.assertTrue(
            any("FUNCTION" in t for t in object_types),
            f"Expected FUNCTION in share updates, got: {object_types}"
        )


class TestTargetManager(unittest.TestCase):
    @patch('target_manager.WorkspaceClient')
    def test_run_target(self, mock_wc):
        """Test target flow: create catalog from share, discover model, deploy endpoint."""
        from target_manager import TargetManager

        mock_w = mock_wc.return_value
        mock_w.catalogs.get.side_effect = Exception("Catalog does not exist")
        mock_w.serving_endpoints.get.side_effect = Exception("Endpoint does not exist")

        mock_schema = MagicMock()
        mock_schema.name = "default"
        mock_w.schemas.list.return_value = [mock_schema]

        mock_model = MagicMock()
        mock_model.full_name = "shared_catalog.default.my_model"
        mock_w.registered_models.list.return_value = [mock_model]

        manager = TargetManager(
            "https://target.cloud.databricks.com",
            "dapi-target-token",
        )
        manager.run(
            share_name="my_share",
            provider_name="my_provider",
            target_catalog="shared_catalog",
            deploy_serving="true",
            serving_endpoint_name="my-endpoint",
        )

        mock_w.catalogs.create.assert_called_with(
            name="shared_catalog",
            provider_name="my_provider",
            share_name="my_share",
        )
        mock_w.serving_endpoints.create.assert_called()


class TestTargetManagerFunctionDeps(unittest.TestCase):
    @patch('target_manager.WorkspaceClient')
    def test_discover_model_finds_functions(self, mock_wc):
        """_discover_model returns function dependencies alongside tables."""
        from target_manager import TargetManager

        mock_w = mock_wc.return_value

        mock_schema = MagicMock()
        mock_schema.name = "default"
        mock_w.schemas.list.return_value = [mock_schema]

        mock_model = MagicMock()
        mock_model.full_name = "shared_cat.default.my_model"
        mock_w.registered_models.list.return_value = [mock_model]

        mock_ver = MagicMock()
        mock_ver.version = 1
        mock_w.model_versions.list.return_value = [mock_ver]

        # Model version with table + function deps
        mock_table_dep = MagicMock()
        mock_table_dep.table.table_full_name = "cat.sch.features"
        mock_table_dep.function = None

        mock_func_dep = MagicMock()
        mock_func_dep.table = None
        mock_func_dep.function.function_full_name = "cat.sch.transform_udf"

        mock_mv = MagicMock()
        mock_mv.model_version_dependencies.dependencies = [mock_table_dep, mock_func_dep]
        mock_w.model_versions.get.return_value = mock_mv

        mgr = TargetManager("https://host.databricks.com", "dapi-xxx")
        info = mgr._discover_model("shared_cat")

        self.assertIsNotNone(info)
        self.assertEqual(info["functions"], ["cat.sch.transform_udf"])
        self.assertEqual(info["feature_tables"], ["cat.sch.features"])


if __name__ == '__main__':
    unittest.main()
