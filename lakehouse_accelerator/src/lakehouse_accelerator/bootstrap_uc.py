import click
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

@click.command()
@click.option("--structure", required=True, help="Path to YAML file with catalog + schema structure")
@click.option("--location-root", required=False, help="Base S3 path for catalog storage (optional)")
@click.option("--dry-run", is_flag=True, help="Print actions without executing")
def main(structure, location_root, dry_run):
    w = WorkspaceClient()

    # Load and validate YAML
    try:
        with open(structure, "r") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        raise click.ClickException(f"Failed to load YAML: {e}")

    if not isinstance(cfg, dict):
        raise click.ClickException("YAML must be a dictionary with 'catalogs' and 'schemas' keys")

    catalogs = cfg.get("catalogs", [])
    schemas = cfg.get("schemas", [])

    if not isinstance(catalogs, list) or not isinstance(schemas, list):
        raise click.ClickException("'catalogs' and 'schemas' must be lists")

    # Create catalogs
    for catalog in catalogs:
        name = catalog["name"]
        comment = catalog.get("comment", "")
        try:
            w.catalogs.get(name=name)
            print(f"✅ Catalog '{name}' already exists.")
        except NotFound:
            if dry_run:
                print(f"[DRY RUN] Would create catalog '{name}'")
            else:
                storage_location = f"{location_root}/{name}" if location_root else None
                w.catalogs.create(
                    name=name,
                    comment=comment,
                    storage_location=storage_location
                )
                print(f"🆕 Catalog '{name}' created.")
        except Exception as e:
            print(f"❌ Failed to create catalog '{name}': {e}")

    # Create schemas
    for schema in schemas:
        name = schema["name"]
        catalog = schema["catalog"]
        full_name = f"{catalog}.{name}"
        try:
            w.schemas.get(full_name)
            print(f"✅ Schema '{full_name}' already exists.")
        except NotFound:
            if dry_run:
                print(f"[DRY RUN] Would create schema '{full_name}'")
            else:
                w.schemas.create(name=name, catalog_name=catalog)
                print(f"🆕 Schema '{full_name}' created.")
        except Exception as e:
            print(f"❌ Failed to create schema '{full_name}': {e}")

if __name__ == "__main__":
    main()
