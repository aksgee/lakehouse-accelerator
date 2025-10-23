import click
import yaml
from databricks.sdk import WorkspaceClient

@click.command()
@click.option("--structure", required=True, help="Path to YAML file with catalog + schema structure")
def main(structure):
    w = WorkspaceClient()
    with open(structure, "r") as f:
        cfg = yaml.safe_load(f)

    for catalog in cfg.get("catalogs", []):
        name = catalog["name"]
        comment = catalog.get("comment", "")
        try:
            w.catalogs.get(name=name)
            print(f"Catalog '{name}' already exists.")
        except:
            w.catalogs.create(name=name, comment=comment)
            print(f"Catalog '{name}' created.")

    for schema in cfg.get("schemas", []):
        name = schema["name"]
        catalog = schema["catalog"]
        full_name = f"{catalog}.{name}"
        try:
            w.schemas.get(full_name)
            print(f"Schema '{full_name}' already exists.")
        except:
            w.schemas.create(name=name, catalog_name=catalog)
            print(f"Schema '{full_name}' created.")

if __name__ == "__main__":
    main()
