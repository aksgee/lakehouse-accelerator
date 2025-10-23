import argparse
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--structure", required=True, help="Path to YAML file with catalog + schema structure")
    parser.add_argument("--grants", required=True, help="Path to YAML file with grant definitions")
    args = parser.parse_args()

    w = WorkspaceClient()

    with open(args.structure, "r") as f:
        cfg = yaml.safe_load(f)
    with open(args.grants, "r") as f:
        grant_defs = yaml.safe_load(f)

    catalog_names = [c["name"] for c in cfg.get("catalogs", [])]
    schema_defs = cfg.get("schemas", [])

    for grant in grant_defs:
        principal = grant["principal"]
        privileges = grant["privileges"]

        for catalog in catalog_names:
            try:
                w.grants.update(full_name=catalog, privileges=privileges, principal=principal)
                print(f"✅ Granted {privileges} on catalog '{catalog}' to '{principal}'")
            except Exception as e:
                print(f"❌ Failed to grant on catalog '{catalog}': {e}")

        for schema in schema_defs:
            full_name = f"{schema['catalog']}.{schema['name']}"
            try:
                w.grants.update(full_name=full_name, privileges=privileges, principal=principal)
                print(f"✅ Granted {privileges} on schema '{full_name}' to '{principal}'")
            except Exception as e:
                print(f"❌ Failed to grant on schema '{full_name}': {e}")

if __name__ == "__main__":
    main()
