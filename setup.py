from setuptools import setup, find_packages

setup(
    name="cross_region_model_serving",
    version="0.1.0",
    author="Debu Sinha",
    author_email="debusinha2009@gmail.com",
    description="Cross Region Model Serving with Databricks Asset Bundles",
    py_modules=["demo_setup", "main", "source_manager", "target_manager", "utils", "interactive_setup", "mcp_server"],
    package_dir={"": "src"},
    entry_points={
        "console_scripts": [
            "main=main:main",
            "demo_setup=demo_setup:main",
            "interactive_setup=interactive_setup:main",
            "cross-region-mcp=mcp_server:main"
        ]
    },
    install_requires=[
        "databricks-sdk",
        "mlflow",
        "databricks-feature-engineering>=0.13.0",
        "pandas",
        "scikit-learn",
        "pyyaml",
        "mcp>=1.0.0"
    ]
)
