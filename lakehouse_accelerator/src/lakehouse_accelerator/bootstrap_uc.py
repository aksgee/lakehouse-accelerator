import argparse
import yaml
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--structure", required=True, help="Path to YAML file with catalog + schema structure")
    parser.add_argument("--location-root", required=False, help="Base S3 path for catalog storage")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    with open(args.structure, "r") as f:
        cfg = yaml.safe_load(f)

    for catalog in cfg.get("catalogs", []):
        name = catalog["name"]
        comment = catalog.get("comment", "")
        location = f"{args.location_root}/{name}" if args.location_root else None

        sql = f"CREATE CATALOG IF NOT EXISTS {name}"
        if location:
            sql += f" MANAGED LOCATION '{location}'"
        if comment:
            sql += f" COMMENT '{comment}'"

        if args.dry_run:
            print(f"[DRY RUN] Would execute: {sql}")
        else:
            spark.sql(sql)

    for schema in cfg.get("schemas", []):
        name = schema["name"]
        catalog = schema["catalog"]
        full_name = f"{catalog}.{name}"
        sql = f"CREATE SCHEMA IF NOT EXISTS {full_name}"

        if args.dry_run:
            print(f"[DRY RUN] Would execute: {sql}")
        else:
            spark.sql(sql)

if __name__ == "__main__":
    main()
