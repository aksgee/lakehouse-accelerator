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
        catalog_privs = grant.get("catalog_privileges", [])
        schema_privs = grant.get("schema_privileges", [])

        for catalog in catalog_names:
            if catalog_privs:
                sql = f"GRANT {', '.join(catalog_privs)} ON CATALOG {catalog} TO `{principal}`"
                if args.dry_run:
                    print(f"[DRY RUN] Would execute: {sql}")
                else:
                    spark.sql(sql)
                    print(f"✅ Granted {catalog_privs} on catalog '{catalog}' to '{principal}'")

        for schema in schema_defs:
            full_name = f"{schema['catalog']}.{schema['name']}"
            if schema_privs:
                sql = f"GRANT {', '.join(schema_privs)} ON SCHEMA {full_name} TO `{principal}`"
                if args.dry_run:
                    print(f"[DRY RUN] Would execute: {sql}")
                else:
                    spark.sql(sql)
                    print(f"✅ Granted {schema_privs} on schema '{full_name}' to '{principal}'")

if __name__ == "__main__":
    main()