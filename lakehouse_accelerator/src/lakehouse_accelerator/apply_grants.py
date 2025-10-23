import argparse
import yaml
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--structure", required=True, help="Path to YAML file with catalog + schema structure")
    parser.add_argument("--grants", required=True, help="Path to YAML file with grant definitions")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

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
            sql = f"GRANT {', '.join(privileges)} ON CATALOG {catalog} TO `{principal}`"
            if args.dry_run:
                print(f"[DRY RUN] Would execute: {sql}")
            else:
                spark.sql(sql)
                print(f"✅ Granted {privileges} on catalog '{catalog}' to '{principal}'")

        for schema in schema_defs:
            full_name = f"{schema['catalog']}.{schema['name']}"
            sql = f"GRANT {', '.join(privileges)} ON SCHEMA {full_name} TO `{principal}`"
            if args.dry_run:
                print(f"[DRY RUN] Would execute: {sql}")
            else:
                spark.sql(sql)
                print(f"✅ Granted {privileges} on schema '{full_name}' to '{principal}'")

if __name__ == "__main__":
    main()
