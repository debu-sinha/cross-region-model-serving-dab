# Dependency-Aware Cross-Region Deployment: An Open Architecture for Automated Multi-Region ML Model Serving

*By Debu Sinha, Lead Specialist Solutions Architect (AI/ML), Databricks*

## Abstract

Production ML models are rarely standalone artifacts. A model registered in Unity Catalog typically depends on feature tables for real-time lookup, Python UDFs for on-demand feature computation, and connections to online feature stores. When an organization needs to serve that model in a different region -- for latency, regulatory, or disaster recovery reasons -- every dependency must be discovered, transferred, provisioned, and wired up in the correct order on the target workspace. Today, most teams do this manually for each model and each region, a process that is slow, fragile, and does not scale.

This article introduces **Dependency-Aware Cross-Region Deployment**, an open-source architecture and reference implementation that automates the entire cross-region model serving pipeline on Databricks. The system uses Delta Sharing's Databricks-to-Databricks (D2D) protocol for zero-copy model and data transfer, automatically discovers all model dependencies through the `model_version_dependencies` API, provisions Lakebase online tables with correct ordering guarantees, and deploys serving endpoints -- all from a single `databricks bundle run` command. The implementation also documents a previously undocumented behavior: Unity Catalog resolves feature lookups by internal table UUIDs rather than catalog name strings, which makes model artifacts portable across workspaces without re-logging or path remapping.

The full implementation is open source at [github.com/debu-sinha/cross-region-model-serving-dab](https://github.com/debu-sinha/cross-region-model-serving-dab).

## The Problem: Cross-Region Model Serving at Enterprise Scale

Every enterprise I work with eventually hits the same wall. They have trained a model in us-east-1 (or whichever region houses their data lake), and now their APAC customers need predictions served from ap-southeast-1. Their European operations need an endpoint in eu-west-1. Their disaster recovery plan requires a hot standby in a second region.

The reasons vary, but they fall into three buckets that show up over and over in enterprise ML deployments:

**Inference latency.** A recommendation model trained on centralized data needs to serve predictions within 10ms in every region where the product operates. Routing inference traffic across continents adds 100-300ms of network latency alone. You cannot meet a 10ms SLA from a single region when your users are distributed globally.

**Data residency and regulatory compliance.** GDPR, CCPA, and industry-specific regulations increasingly dictate where model predictions can be generated, not just where training data is stored. A healthcare model trained in the US may need to serve predictions in the EU, with the inference endpoint (and its feature data) physically located in an EU data center.

**Disaster recovery.** A single-region serving deployment is a single point of failure. Multi-region serving provides a hot standby that can absorb traffic within minutes if the primary region goes down. For models backing revenue-critical applications -- fraud detection, pricing, search ranking -- this is not optional.

These are not edge cases. According to a 2024 Gartner survey, 40% of enterprises running production ML will require multi-region model serving by 2027, up from roughly 15% in 2023. A separate survey from Tecton found that among ML teams already operating across multiple regions, 60% rely on custom-built tooling and fewer than 20% reported being satisfied with their current approach.

The gap between "we need to serve this model in another region" and "the model is actually serving predictions in that region" is where teams lose days or weeks. Not because the individual steps are hard, but because there are many steps, they have ordering dependencies, and the failure modes are subtle. A model that depends on three feature tables, two Python UDFs, and an online feature store requires all of those components to be present, synchronized, and online in the target region before the serving endpoint will function. Miss one table. Forget one UDF. Deploy the endpoint before the online tables finish syncing. Any of these produces an endpoint that fails to start or returns errors at inference time, and the error messages rarely point to the actual root cause.

## Background: Delta Sharing and Unity Catalog

Before diving into the architecture, it is worth grounding the discussion in Delta Sharing for readers who have not worked with it directly.

### What Delta Sharing Is

Delta Sharing is an open protocol for secure data sharing, originally published by Databricks in 2021 and donated to the Linux Foundation. The protocol defines a REST-based interface that allows a data provider to grant a recipient read access to datasets without copying the data. The recipient reads directly from the provider's cloud storage through short-lived, pre-signed URLs. No data movement, no ETL pipelines, no storage duplication.

The protocol has two implementations that matter for cross-region ML:

**Open Sharing** uses token-based authentication and works with any recipient, including non-Databricks platforms. It supports sharing Delta tables (snapshots and change data feed), but it does not support sharing ML models, Python UDFs, or views. If your recipient is running Spark on a different platform or a pandas-based application, Open Sharing is what you would use -- but it cannot share a model.

**Databricks-to-Databricks (D2D) Sharing** works between Databricks workspaces that both have Unity Catalog enabled. It supports everything Open Sharing supports, plus ML models, Python UDFs (shared as FUNCTION objects), views, and volumes. D2D sharing uses automatic credential vending between the two workspaces' Unity Catalog metastores rather than static tokens. Within the same cloud provider, D2D sharing is zero-copy -- the recipient workspace reads from the provider's storage directly through credential vending. Cross-cloud sharing (e.g., AWS to Azure) uses VPC peering and does involve a network hop but still no data duplication.

D2D sharing propagates model versions to the recipient within seconds of registration on the provider side. This makes it fast enough for production deployment pipelines.

### What Delta Sharing Does Not Do

Here is the critical gap that motivated this project: **Delta Sharing does not automatically include a model's dependencies.**

When you add a model to a Delta Share, only the model artifact itself is shared. The feature tables that the model was trained with -- the ones it needs for real-time feature lookup at serving time -- are NOT included. The Python UDFs that compute on-demand features at inference time are NOT included. The online feature store configuration is not transferable at all.

You have to manually:

1. Inspect the model's version metadata to find its dependencies
2. Add each feature table to the share as a TABLE object
3. Add each Python UDF to the share as a FUNCTION object (D2D only)
4. On the recipient side, create a shared catalog from the share
5. Provision a new online feature store (Lakebase)
6. Publish each shared feature table as an online table
7. Wait for every online table to reach ONLINE state
8. Deploy the serving endpoint

Get any of these wrong -- miss a dependency, skip the wait, deploy in the wrong order -- and you get a broken endpoint. This is the problem I set out to solve.

### Unity Catalog's Role

Unity Catalog is the governance layer that ties all of this together. Models, feature tables, Python UDFs, shares, and recipients are all Unity Catalog objects with ACLs, lineage tracking, and audit logging. The `model_version_dependencies` API -- which is central to this architecture -- is a Unity Catalog API that returns the full dependency graph for a given model version: every table and function the model references.

Shares and recipients are metastore-level objects in Unity Catalog. A single metastore can serve multiple workspaces (common for dev/staging/prod within one organization). When the source and target workspaces share the same metastore, Delta Sharing uses a special built-in `self` recipient rather than creating a D2D trust relationship. The architecture handles both cases automatically.

## Prior Approaches and Their Limitations

Before describing the architecture, it is useful to understand what teams are doing today and why those approaches fall short.

**Manual replication.** The most common approach. An MLOps engineer SSHs into the target workspace, manually creates the shared catalog, publishes online tables, and deploys the endpoint. This works once. It breaks down when you have multiple models, multiple regions, or models that retrain on a schedule. I have seen teams spend two to three days deploying a single model to a second region, then repeat the entire process when the model is retrained with new features.

**Platform-native tools.** AWS SageMaker has multi-model endpoints but no built-in multi-region deployment. Google Vertex AI supports regional endpoints but requires you to manually copy model artifacts and set up feature stores per region. Azure ML has no multi-region model serving primitive. None of these platforms automatically handle the dependency graph -- they all treat the model as an isolated artifact.

**Custom CI/CD pipelines.** Some mature ML platform teams build Terraform or Pulumi modules that provision infrastructure across regions. This handles the infrastructure layer but not the data layer. You still need to figure out how to get the feature tables and UDFs to the target region, and you still need to handle the ordering constraints around online table readiness. Most teams end up with fragile scripts that hard-code model-specific dependencies.

**Internal frameworks at large tech companies.** Netflix, Uber, and Airbnb have all published blog posts describing their internal multi-region ML serving infrastructure. These systems took dedicated platform teams months to years to build, are tightly coupled to their internal stack, and are not available as open-source or off-the-shelf solutions. They demonstrate that the problem is real and important, but they do not help the vast majority of ML teams.

The gap in all of these approaches is the same: nobody is solving the dependency discovery and ordered provisioning problem in a way that is automated, general-purpose, and open source. That is the gap this architecture fills.

## Architecture: Dependency-Aware Cross-Region Deployment

The core idea behind this architecture is that a model's deployment should be driven by its dependency graph, not by manual checklists. The system inspects the model, discovers everything it depends on, transfers all dependencies to the target region, provisions the online infrastructure in the correct order, and deploys the serving endpoint -- all without human intervention after the initial configuration.

![Architecture Diagram](architecture-diagram.svg)

The architecture has two sides connected by Delta Sharing's D2D protocol:

**Provider workspace** (training region) -- This is where the model was trained and registered in Unity Catalog. The provider side is responsible for:
- Extracting the model's dependency graph via the `model_version_dependencies` API
- Creating a Delta Share containing the model, its feature tables (as TABLE objects), and its Python UDFs (as FUNCTION objects)
- Granting the recipient workspace access to the share

**Recipient workspace** (serving region) -- This is where the model needs to serve predictions. The recipient side handles:
- Creating a shared catalog from the Delta Share (read-only)
- Discovering the model and its dependencies within the shared catalog
- Provisioning a Lakebase online store and publishing feature tables as online tables in a separate writable catalog
- Waiting for all online tables to reach ONLINE state (non-negotiable ordering constraint)
- Deploying the serving endpoint with correct feature store configuration

The D2D bridge between them is zero-copy within the same cloud. Model versions propagate in seconds. Feature table data is accessed through credential vending with no storage duplication.

### Why the Dependency Graph Matters

Consider a fraud detection model registered in Unity Catalog. Through `model_version_dependencies`, we discover it depends on:

- `catalog.schema.user_features` -- a feature table with user transaction history (FeatureLookup)
- `catalog.schema.compute_risk_score` -- a Python UDF that computes a risk score on-demand at inference time (FeatureFunction)

If we share only the model, the serving endpoint in the target region will fail at inference time because it cannot find the feature table or the UDF. If we share the model and the feature table but forget the UDF, the endpoint starts but returns errors for any prediction that triggers the on-demand computation.

The automation treats the model's dependency graph as the source of truth. It queries `model_version_dependencies`, iterates through every dependency, classifies each one (TABLE or FUNCTION), and adds all of them to the share before proceeding to the recipient side.

## Implementation: A Three-Task Pipeline

The bundle runs a three-task Databricks job with enforced task dependencies. The ordering is deliberate -- getting this sequence wrong is the single most common failure mode in manual cross-region deployments.

![DAB Pipeline](pipeline-diagram.svg)

### Task 1: Demo Setup (optional)

Creates a sample model with a feature table, FeatureLookup integration, and a FeatureFunction (Python UDF). This exists so you can validate the entire pipeline end-to-end without bringing your own model. Remove this task when deploying against your production models.

### Task 2: Source Share Setup

This is where the dependency discovery and sharing happens. The `SourceManager` class:

1. Retrieves the latest version of the specified model from Unity Catalog
2. Calls `model_version_dependencies` to get the full dependency list
3. Classifies each dependency as TABLE or FUNCTION
4. Creates (or updates) a Delta Share
5. Adds the model, all feature tables, and all Python UDFs to the share
6. Detects whether the source and target workspaces share the same Unity Catalog metastore. If they do, it uses the built-in `self` recipient (avoiding a "same sharing identifier" error that occurs when trying to create a D2D recipient within the same metastore). If they have different metastores, it creates a proper D2D recipient with token-based or credential-vended authentication.
7. Grants the recipient SELECT access to the share

Target workspace credentials are resolved from a Databricks secret scope at runtime. The code detects `{{secrets/scope/key}}` patterns in configuration and calls `dbutils.secrets.get()` on the cluster, so credentials never appear in job parameters, logs, or the bundle configuration file.

### Task 3: Target Registration

This is the most involved step. The `TargetManager` class runs four phases in strict order:

**Phase 1: Catalog setup.** Creates a shared catalog from the Delta Share on the target workspace. The catalog is read-only -- you can query its tables and use its models, but you cannot write to it. For enterprises with strict RBAC where ML engineers do not have CREATE CATALOG permissions, the bundle supports a `use_existing_catalog: true` mode where an admin pre-creates the catalog.

**Phase 2: Model discovery.** Scans the shared catalog, finds the model, extracts its latest version, and re-discovers its dependencies within the shared catalog's namespace. This second dependency extraction (on the target side) ensures the pipeline adapts if dependencies changed between the source share setup and target registration.

**Phase 3: Online table provisioning.** This is where most manual deployments fail. Shared catalogs are read-only, so you cannot create online tables inside them. Online tables must go in a separate writable catalog on the target workspace. The automation:

- Creates a Lakebase online store (capacity: CU_1) if one does not exist
- For each feature table, publishes it as an online table in the writable catalog with an `_online` suffix
- Polls each online table every 30 seconds, waiting for the `detailed_state` to report ONLINE
- Will not proceed past this phase until ALL online tables are ONLINE (600-second timeout)
- Detects terminal failure states (FAILED, OFFLINE_FAILED) and aborts with a clear error

This wait is non-negotiable. If you deploy a serving endpoint before online tables are ready, it fails with "Online feature store setup failed." The error message gives no indication that this is a timing problem, which makes it one of the most frustrating issues to debug in manual deployments.

**Phase 4: Endpoint deployment.** Creates (or updates) a model serving endpoint on the target workspace. The endpoint is configured with `scale_to_zero=True` and `workload_size=Small` by default. It handles the common "update already in progress" conflict by waiting for the existing update to complete before applying the new configuration.

## A Discovery That Makes Model Portability Work

When I first built this system, I expected feature lookups to break after sharing. The reason seemed obvious: a model trained against `prod_catalog.ml.user_features` would see its feature table as `shared_catalog.ml.user_features` in the target workspace. Different catalog name, same schema and table name. I assumed the serving endpoint would try to look up features using the original catalog path and fail.

I was wrong.

After testing, I found that Unity Catalog resolves feature lookups using internal table IDs (UUIDs), not catalog name strings. When the model was trained with a FeatureLookup pointing to `prod_catalog.ml.user_features`, the model artifact stores the table's UUID, not its string path. When `publish_table()` creates an online table from the shared feature table, it sets `source_table_id` to that same UUID. At inference time, the serving endpoint matches features by UUID, not by catalog path.

This means the model artifact is portable across workspaces without modification. No re-logging, no path remapping, no "fix up feature references" step. The pipeline simply shares the model and its dependencies, publishes online tables from the shared data, and the UUIDs handle the rest.

This behavior is not documented in the Databricks feature engineering documentation (as of early 2026). I confirmed it through testing across multiple workspace configurations: same-metastore sharing, cross-metastore D2D sharing, and different-cloud D2D sharing. In all cases, UUID-based resolution worked correctly.

For practitioners building their own cross-region pipelines, this is the single most important implementation detail to understand. It means the dependency graph is fully portable -- you do not need to modify the model or its feature specifications to deploy in a new region.

## Enterprise Configuration

Everything is parameterized through `databricks.yml`. A platform team can fork this bundle, set organization-specific defaults, and give it to ML engineers as a self-service deployment tool.

```yaml
variables:
  secret_scope:
    default: my_cross_region_secrets    # Secret scope with target credentials
  model_name:
    default: main.default.model_${workspace.current_user.short_name}
  provider_name:
    default: self                       # 'self' for same-metastore, D2D otherwise
  create_online_table:
    default: "true"                     # Set false if model has no feature lookups
  online_table_target_catalog:
    default: main                       # Writable catalog for online tables
  online_table_target_schema:
    default: default
```

The bundle ships with two targets: `dev` (interactive development with existing clusters) and `prod` (production deployment with job clusters). The full variable list supports customization of every aspect of the pipeline without modifying source code.

### Production Quick Start

```bash
git clone https://github.com/debu-sinha/cross-region-model-serving-dab.git
cd cross-region-model-serving-dab

# Store target workspace credentials in a secret scope
databricks secrets create-scope my_cross_region_secrets
databricks secrets put-secret my_cross_region_secrets host \
  --string-value "https://target-workspace.cloud.databricks.com"
databricks secrets put-secret my_cross_region_secrets token \
  --string-value "YOUR_TARGET_WORKSPACE_TOKEN"

# Deploy and run -- one command
databricks bundle deploy -t dev
databricks bundle run -t dev cross_region_model_share_job
```

## Operational Tradeoffs and Known Constraints

Every architecture has boundaries. These are the ones that matter for production planning.

**Snapshot-only sync for shared feature tables.** Online tables created from Delta Shared feature tables only support Snapshot sync mode. Triggered and Continuous sync modes are not available for shared table sources. Feature data in the target region is as fresh as the last snapshot. For features that update daily or less frequently, this is fine. For features that change every few minutes, you will need to run the source feature pipeline in the target region directly.

**Shared catalogs are read-only.** This is the most common source of confusion during initial setup. Online tables cannot be created inside a shared catalog. They must go in a separate writable catalog on the target workspace. The `online_table_target_catalog` parameter controls this, and the pipeline handles the mapping automatically, but it surprises teams who expect to keep everything in one catalog.

**Table UUID stability.** Because feature lookup resolution is UUID-based, dropping and recreating a source feature table (even with the same name and schema) breaks the link. The UUID changes, and the online table in the target workspace no longer maps to the correct source. Remediation: use `UNDROP TABLE WITH ID` to restore the original table, or re-run the full pipeline to re-publish.

**D2D sharing required for models and UDFs.** ML models, Python UDFs, and FeatureSpecs can only be shared through D2D sharing. Open Sharing (token-based) supports tabular data only. Both workspaces must have Unity Catalog enabled, and they must be on compatible Databricks platform versions.

**Cost.** Lakebase online stores are billed by provisioned capacity (CU_1 is the minimum). Serving endpoints are billed while running -- scale-to-zero helps but does not eliminate cost entirely (there is a cold-start latency tradeoff). Delta Sharing within the same cloud provider carries no additional charge. When tearing down, destroy resources in order: endpoint first, then online tables, then online store, then catalog.

### Schema-Level Sharing for Mature Pipelines

For production ML pipelines that evolve frequently, consider sharing at the schema level instead of individual objects. When you share an entire schema, new models, tables, and volumes added to that schema are automatically visible to recipients without re-running the pipeline. The bundle currently shares individual objects for maximum control and auditability, but schema-level sharing is the right choice when you want a "share everything in this ML namespace" pattern.

## Extending the Pattern: MCP Server for AI-Assisted Operations

Beyond the batch automation, the project includes an MCP (Model Context Protocol) server that exposes the sharing and consumption workflows as tools for AI coding assistants. The DAB handles scheduled, automated deployments. The MCP server enables ad-hoc, conversational operations -- inspecting model dependencies, checking endpoint status, or setting up a share interactively.

The server exposes eight tools:

| Tool | Purpose |
|------|---------|
| `share_model` | Set up Delta Sharing for a model and all its dependencies |
| `consume_shared_model` | Run the full target pipeline (catalog, online tables, endpoint) |
| `inspect_model_dependencies` | Show what feature tables and functions a model depends on |
| `list_shares` / `list_recipients` | Read-only workspace inspection |
| `get_share_details` | Get a share's objects and current permissions |
| `check_endpoint_status` | Check if a serving endpoint is ready for traffic |
| `validate_target_configuration` | Pre-validate config before running a deployment |

An engineer can connect this to Claude Desktop, Cursor, or VS Code and ask questions like "What feature tables does my fraud model depend on?" or "Check if the APAC endpoint is ready." Every tool supports explicit workspace credentials, so you can inspect and operate across multiple workspaces from a single MCP client session.

This represents a broader pattern I am exploring: wrapping infrastructure automation as MCP tools so that AI assistants can become operational copilots for ML platform work, not just code generation tools.

## Broader Applicability

While this implementation targets Databricks, the Dependency-Aware Cross-Region Deployment pattern is transferable to any ML platform that meets two conditions:

1. A model registry that tracks dependencies (equivalent to `model_version_dependencies`)
2. A data sharing mechanism that preserves object identity across workspaces (equivalent to UUID-based resolution in Delta Sharing)

The core algorithm -- inspect model, discover dependencies, transfer everything, provision online infrastructure in order, deploy endpoint -- is platform-agnostic. What changes between platforms is the specific API calls and the data transfer mechanism.

For teams not on Databricks, the architecture still provides a useful blueprint. The dependency discovery step, the ordered provisioning with readiness checks, and the UUID-based portability insight all apply regardless of which model registry and feature store you are using. If your platform does not have built-in dependency tracking, you can implement it through model metadata tags or a separate dependency manifest.

## Future Work

There are several extensions I am actively working on:

**FeatureSpec sharing.** FeatureSpecs define the full feature retrieval pipeline -- which tables to look up, which UDFs to call, in what order. They are Unity Catalog objects but are not yet automated in the sharing pipeline. Adding FeatureSpec support would make the dependency graph truly complete.

**Triggered sync for shared tables.** The current snapshot-only constraint means feature freshness in the target region depends on how often you trigger a snapshot. Databricks has been working on triggered and continuous sync modes for shared table sources -- when these become available for shared tables, the architecture will support near-real-time feature consistency across regions.

**Multi-model consolidated shares.** Right now, each model gets its own Delta Share. When multiple models share common feature tables (which is common in practice -- a fraud model and a risk model might both use the same user transaction features), the pipeline creates redundant shares for the same underlying data. Consolidating related models into a single share with deduplicated dependencies would reduce management overhead.

**Automated drift detection.** When a model is retrained with new features, the dependency graph changes. An automated system that detects dependency drift between what is currently shared and what the latest model version needs would close the loop on continuous deployment.

## Getting Started

The full implementation, documentation, and example notebooks are at [github.com/debu-sinha/cross-region-model-serving-dab](https://github.com/debu-sinha/cross-region-model-serving-dab). The README includes step-by-step setup instructions, configuration reference, and troubleshooting guides for common issues.

If you are building multi-region model serving infrastructure, I would like to hear about your experience. File issues, send PRs, or reach out directly. The whole point of open-sourcing this is so teams do not have to solve this problem from scratch.

## References

1. Delta Sharing: An Open Protocol for Secure Data Sharing. Linux Foundation, 2021. [delta.io/sharing](https://delta.io/sharing)
2. Databricks Unity Catalog Documentation: Delta Sharing. [docs.databricks.com/en/delta-sharing](https://docs.databricks.com/en/delta-sharing/index.html)
3. Databricks Feature Engineering: FeatureLookup and FeatureFunction. [docs.databricks.com/en/machine-learning/feature-store](https://docs.databricks.com/en/machine-learning/feature-store/index.html)
4. Databricks Lakebase (Online Tables). [docs.databricks.com/en/machine-learning/feature-store/online-tables](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html)
5. Databricks Asset Bundles. [docs.databricks.com/en/dev-tools/bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html)
6. Model Context Protocol (MCP). [modelcontextprotocol.io](https://modelcontextprotocol.io)

---

*Debu Sinha is a Lead Specialist Solutions Architect for AI/ML at Databricks. He works with enterprise ML teams on production model serving, feature engineering, and evaluation infrastructure. He is the author of [Practical Machine Learning on Databricks](https://www.packtpub.com/product/practical-machine-learning-on-databricks/9781801812030) (Packt, 2023) and an IEEE Senior Member. He holds an MS from Johns Hopkins University, where he conducted research at the Center for Language and Speech Processing.*
