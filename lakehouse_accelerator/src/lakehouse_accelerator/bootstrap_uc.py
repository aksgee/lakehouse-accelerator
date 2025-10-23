import click
import yaml
from pyspark.sql import SparkSession

@click.command()
@click.option("--structure", required=True, help="Path to YAML file with catalog + schema structure")
@click.option("--location-root", required=False, help="Base S3 path for catalog storage (optional)")
@click.option("--dry-run", is_flag=True, help="Print actions without executing")
def main(structure, location_root, dry_run):
    spark = SparkSession.builder.getOrCreate()

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

    # Create catalogs
    for catalog in catalogs:
        name = catalog["name"]
        comment = catalog.get("comment", "")
        location = f"{location_root}/{name}" if location_root else None

        sql = f"CREATE CATALOG IF NOT EXISTS {name}"
        if location:
            sql += f" MANAGED LOCATION '{location}'"
        if comment:
            sql += f" COMMENT '{comment}'"

        if dry_run:
            print(f"[DRY RUN] Would execute: {sql}")
        else:
            print(f"🆕 Creating catalog: {name}")
            spark.sql(sql)

    # Create schemas
    for schema in schemas:
        name = schema["name"]
        catalog = schema["catalog"]
        full_name = f"{catalog}.{name}"

        sql = f"CREATE SCHEMA IF NOT EXISTS {full_name}"

        if dry_run:
            print(f"[DRY RUN] Would execute: {sql}")
        else:
            print(f"🆕 Creating schema: {full_name}")
            spark.sql(sql)

if __name__ == "__main__":
    main()
