# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2025-12-05

### Added
- End-to-end DAB for sharing ML models across Databricks workspaces via Delta Sharing
- Online feature table sync to Lakebase in the target workspace
- Serving endpoint deployment with feature lookup wiring
- Same-metastore sharing via `provider_name: self`
- Cross-metastore D2D recipient creation with automatic metastore ID detection
- All settings configurable through `databricks.yml` variables
- Demo setup task that creates a sample model with feature table
- Architecture diagram and step-by-step README
- Support for users without CREATE CATALOG permission (`use_existing_catalog`)
- Works on AWS, Azure, and GCP

### Fixed
- Lint errors caught by ruff (unused imports, f-strings without placeholders)
- Test mocks aligned with current Databricks SDK (`metastores.summary()` instead of `.current()`)

### Repository
- Added LICENSE, SECURITY.md, CONTRIBUTING.md, CITATION.cff
- Added GitHub Actions CI (lint + tests on Python 3.10/3.11)
- Added CODEOWNERS and PR template

## [0.1.0] - 2025-12-02

### Added
- Initial implementation
- Basic Delta Sharing workflow
- Source and target workspace management
