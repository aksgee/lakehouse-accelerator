#!/bin/bash

# Create base repo folder
mkdir -p lakehouse-accelerator
cd lakehouse-accelerator

# Create top-level folders
mkdir -p utilities orchestrator shared_rules/dq_templates shared_rules/governance_templates manifest tables

# Create utility scripts
touch utilities/{bootstrap_env.py,logger.py,common.py,dq_utils.py,governance_utils.py,scd_utils.py,ai_translate.py,bootstrap_table.py}

# Create orchestrator scripts
touch orchestrator/{lakeflow_launcher.py,dlt_launcher.py,autoloader_launcher.py,bundle_deployer.py}

# Create shared rule templates
touch shared_rules/dq_templates/{not_null.json,regex_email.json,range_check.json}
touch shared_rules/governance_templates/{pii_masking.json,abac_roles.json,retention_policy.json}

# Create manifest files
touch manifest/{table_registry.json,deployment_matrix.json,bundle_registry.json}

# Create dummy table structure
TABLE_PATH="tables/dummy/dummy/dummy"
mkdir -p $TABLE_PATH

# Subfolders and files
for folder in ingestion transformation dq_rules scd_config governance monitoring policies ownership env/dev env/qa env/prod scripts; do
  mkdir -p $TABLE_PATH/$folder
done

# README and bundle
echo "# Table: dummy" > $TABLE_PATH/README.md
echo -e "bundle:\n  name: dummy_table_bundle\n  target: dev" > $TABLE_PATH/bundle.yml

# Ingestion
touch $TABLE_PATH/ingestion/{ingestion.json,autoloader_settings.json}

# Transformation
touch $TABLE_PATH/transformation/{sql_logic.sql,enrichment_config.json}

# DQ Rules
touch $TABLE_PATH/dq_rules/{technical.json,business.json}

# SCD Config
touch $TABLE_PATH/scd_config/{scd1.json,scd2.json}

# Governance
touch $TABLE_PATH/governance/{technical.json,business.json}

# Monitoring
touch $TABLE_PATH/monitoring/{freshness_sla.json,alert_config.json}

# Policies
touch $TABLE_PATH/policies/{policy_abac.json,policy_lineage.json,policy_retention.json}

# Ownership
touch $TABLE_PATH/ownership/owner.json

# Scripts
touch $TABLE_PATH/scripts/{scd_merge.py,dq_runner.py,governance_applier.py}

echo "✅ lakehouse-accelerator structure created successfully."
