import argparse
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

def bootstrap_uc(config_path: str):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    w = WorkspaceClient()

    for catalog in config.get("catalogs", []):
        name = catalog["name"]
        try:
            w.catalogs.get(name)
            print(f"Catalog '{name}' already exists. Skipping.")
        except NotFound:
            print(f"Creating catalog '{name}'...")
            w.catalogs.create(name=name, comment=catalog.get("comment", ""))

    for schema in config.get("schemas", []):
        name = schema["name"]
        catalog_name = schema["catalog"]
        try:
            w.schemas.get(catalog_name=catalog_name, schema_name=name)
            print(f"Schema '{catalog_name}.{name}' already exists. Skipping.")
        except NotFound:
            print(f"Creating schema '{catalog_name}.{name}'...")
            w.schemas.create(name=name, catalog_name=catalog_name)

    for grant in config.get("grants", []):
        principal = grant["principal"]
        privileges = grant["privileges"]
        catalog_name = config["catalogs"][0]["name"]
        print(f"Applying grants to '{principal}' on '{catalog_name}'...")
        w.grants.update(principal=principal, privileges=privileges, catalog=catalog_name)

    print("✅ Unity Catalog bootstrap completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    bootstrap_uc(args.config)
