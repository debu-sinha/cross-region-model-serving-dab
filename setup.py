from setuptools import setup

setup(
    name="cross_region_model_serving",
    version="0.1.0",
    author="Debu Sinha",
    author_email="debusinha2009@gmail.com",
    description="Cross-region ML model serving with Delta Sharing and an MCP server for AI assistants",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/debu-sinha/cross-region-model-serving-dab",
    py_modules=[
        "demo_setup",
        "main",
        "source_manager",
        "target_manager",
        "utils",
        "interactive_setup",
        "mcp_server",
    ],
    package_dir={"": "src"},
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "main=main:main",
            "demo_setup=demo_setup:main",
            "interactive_setup=interactive_setup:main",
            "cross-region-mcp=mcp_server:main",
        ]
    },
    # Base install: only what the MCP server needs.
    # The cluster-side code (SourceManager, TargetManager, demo_setup)
    # imports heavy packages like mlflow and databricks-feature-engineering
    # at runtime inside Databricks -- they don't need to be installed locally.
    install_requires=[
        "databricks-sdk>=0.20.0",
        "mcp>=1.0.0",
    ],
    extras_require={
        # Install with pip install -e ".[cluster]" when running on a Databricks cluster
        # or locally for the full CLI (source/target managers).
        "cluster": [
            "mlflow>=2.10.0,<4.0.0",
            "databricks-feature-engineering>=0.13.0,<1.0.0",
            "pandas>=2.0.0,<3.0.0",
            "scikit-learn>=1.3.0,<2.0.0",
            "pyyaml",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
