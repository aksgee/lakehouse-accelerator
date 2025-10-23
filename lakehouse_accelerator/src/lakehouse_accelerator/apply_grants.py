import click
import yaml
from databricks.sdk import WorkspaceClient

@click.command()
@click.option("--structure", required=True, help="Path to YAML file with catalog + schema structure")
@click.option("--grants", required=True, help="Path to YAML file with grant definitions")
def main(structure, grants):
    w = WorkspaceClient()

    with open(structure, "r") as f:
        cfg = yaml.safe_load(f)
    with open(grants, "r") as f:
        grant_defs = yaml.safe_load(f)

    catalog_names = [c["name"] for c in cfg.get("catalogs", [])]
    schema_defs = cfg.get("schemas", [])

    for grant in grant_defs:
        principal = grant["principal"]
        privileges = grant["privileges"]

        for catalog in catalog_names:
            w.grants.update(full_name=catalog, privileges=privileges, principal=principal)
            print(f"Granted {privileges} on catalog '{catalog}' to '{principal}'")

        for schema in schema_defs:
            full_name = f"{schema['catalog']}.{schema['name']}"
            w.grants.update(full_name=full_name, privileges=privileges, principal=principal)
            print(f"Granted {privileges} on schema '{full_name}' to '{principal}'")

if __name__ == "__main__":
    main()
