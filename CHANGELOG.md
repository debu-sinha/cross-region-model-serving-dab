# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2025-12-05

### Added
- Production-ready Databricks Asset Bundle for cross-region model serving
- Delta Sharing integration for secure model and feature table transfer
- Automatic online feature table synchronization to Lakebase
- Serving endpoint deployment with feature lookup support
- Support for same-metastore sharing (`provider_name: self`)
- Comprehensive configuration options via `databricks.yml`
- Demo setup task for quick testing
- Architecture diagram and detailed documentation

### Features
- Zero-copy data access for same-cloud deployments
- Automatic feature table dependency detection from ML models
- Configurable online store and serving endpoint settings
- Support for users without CREATE CATALOG permissions
- Cross-cloud provider compatibility (AWS, Azure, GCP)

## [0.1.0] - 2025-12-02

### Added
- Initial implementation
- Basic Delta Sharing workflow
- Source and target workspace management
