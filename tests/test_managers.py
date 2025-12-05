import unittest
from unittest.mock import MagicMock, patch
from src.source_manager import SourceManager
from src.target_manager import TargetManager
from databricks.sdk.service import sharing


class TestSourceManager(unittest.TestCase):
    @patch('src.source_manager.WorkspaceClient')
    @patch('src.source_manager.FeatureEngineeringClient')
    @patch('src.source_manager.mlflow')
    def test_run_source(self, mock_mlflow, mock_fe, mock_wc):
        mock_w = mock_wc.return_value
        mock_w.shares.get.side_effect = Exception("Not found")

        mock_client = MagicMock()
        mock_mlflow.MlflowClient.return_value = mock_client
        mock_version = MagicMock()
        mock_version.version = "1"
        mock_client.search_model_versions.return_value = [mock_version]

        manager = SourceManager()
        manager.run("my.model", "my_share", "my_recipient")

        mock_w.shares.create.assert_called_with(name="my_share")
        mock_w.shares.update.assert_called()

    @patch('src.source_manager.WorkspaceClient')
    @patch('src.source_manager.FeatureEngineeringClient')
    @patch('src.source_manager.mlflow')
    def test_run_source_dynamic_recipient(self, mock_mlflow, mock_fe, mock_wc):
        mock_local_w = MagicMock()
        mock_target_w = MagicMock()
        mock_wc.side_effect = [mock_local_w, mock_target_w]

        mock_metastore = MagicMock()
        mock_metastore.global_metastore_id = "global-id-123"
        mock_target_w.metastores.current.return_value = mock_metastore

        mock_local_w.shares.get.side_effect = Exception("Not found")
        mock_local_w.recipients.get.side_effect = [Exception("Not found"), MagicMock()]

        mock_client = MagicMock()
        mock_mlflow.MlflowClient.return_value = mock_client
        mock_version = MagicMock()
        mock_version.version = "1"
        mock_client.search_model_versions.return_value = [mock_version]

        manager = SourceManager()
        manager.run("my.model", "my_share", "my_recipient", target_host="host", target_token="token")

        mock_target_w.metastores.current.assert_called()
        mock_local_w.recipients.create.assert_called()
        call_args = mock_local_w.recipients.create.call_args
        self.assertIn("data_recipient_global_metastore_id='global-id-123'", str(call_args))


class TestTargetManager(unittest.TestCase):
    @patch('src.target_manager.WorkspaceClient')
    def test_run_target(self, mock_wc):
        mock_w = mock_wc.return_value
        mock_w.catalogs.get.side_effect = Exception("Not found")
        mock_w.serving_endpoints.get.side_effect = Exception("Not found")

        mock_schema = MagicMock()
        mock_schema.name = "default"
        mock_w.schemas.list.return_value = [mock_schema]

        mock_model = MagicMock()
        mock_model.full_name = "my_catalog.default.my_model"
        mock_w.registered_models.list.return_value = [mock_model]

        manager = TargetManager("host", "token")
        manager.run("my_share", "my_provider", "my_catalog", "true", "my_endpoint")

        mock_w.catalogs.create.assert_called_with(name="my_catalog", provider_name="my_provider", share_name="my_share")
        mock_w.serving_endpoints.create.assert_called()


if __name__ == '__main__':
    unittest.main()
