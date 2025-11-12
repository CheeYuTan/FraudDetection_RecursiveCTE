# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Fraud Detection - Analysis and Visualization
# MAGIC 
# MAGIC This notebook provides comprehensive fraud analysis and visualizations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure Parameters

# COMMAND ----------

# Create widgets for user configuration
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "${catalog}.${schema}", "Schema/Database Name")

# Get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Using catalog: {catalog}, schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Fraud Statistics Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_claims,
# MAGIC   SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraudulent_claims,
# MAGIC   ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_percentage,
# MAGIC   SUM(claim_amount) as total_amount,
# MAGIC   SUM(CASE WHEN is_fraud THEN claim_amount ELSE 0 END) as fraudulent_amount,
# MAGIC   ROUND(SUM(CASE WHEN is_fraud THEN claim_amount ELSE 0 END) * 100.0 / SUM(claim_amount), 2) as fraud_amount_percentage,
# MAGIC   AVG(claim_amount) as avg_claim_amount,
# MAGIC   AVG(CASE WHEN is_fraud THEN claim_amount ELSE NULL END) as avg_fraud_amount
# MAGIC FROM ${catalog}.${schema}.claims;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Fraud by Claim Type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   claim_type,
# MAGIC   COUNT(*) as total_claims,
# MAGIC   SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
# MAGIC   ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate,
# MAGIC   SUM(claim_amount) as total_amount,
# MAGIC   SUM(CASE WHEN is_fraud THEN claim_amount ELSE 0 END) as fraud_amount
# MAGIC FROM ${catalog}.${schema}.claims
# MAGIC GROUP BY claim_type
# MAGIC ORDER BY fraud_rate DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Fraud Network Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze fraud rings
# MAGIC SELECT 
# MAGIC   fraud_ring_id,
# MAGIC   COUNT(*) as ring_size,
# MAGIC   COUNT(DISTINCT policyholder_id) as unique_policyholders,
# MAGIC   SUM(claim_amount) as total_ring_amount,
# MAGIC   AVG(claim_amount) as avg_claim_amount,
# MAGIC   MIN(claim_date) as first_claim_date,
# MAGIC   MAX(claim_date) as last_claim_date
# MAGIC FROM ${catalog}.${schema}.claims
# MAGIC WHERE fraud_ring_id IS NOT NULL
# MAGIC GROUP BY fraud_ring_id
# MAGIC ORDER BY total_ring_amount DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Geographic Fraud Patterns

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   p.state,
# MAGIC   p.city,
# MAGIC   COUNT(*) as total_claims,
# MAGIC   SUM(CASE WHEN c.is_fraud THEN 1 ELSE 0 END) as fraud_count,
# MAGIC   ROUND(SUM(CASE WHEN c.is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate,
# MAGIC   SUM(c.claim_amount) as total_amount
# MAGIC FROM ${catalog}.${schema}.claims c
# MAGIC JOIN ${catalog}.${schema}.policyholders p ON c.policyholder_id = p.policyholder_id
# MAGIC GROUP BY p.state, p.city
# MAGIC HAVING fraud_count > 0
# MAGIC ORDER BY fraud_rate DESC, fraud_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Time-based Fraud Trends

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DATE_TRUNC('month', claim_date) as claim_month,
# MAGIC   COUNT(*) as total_claims,
# MAGIC   SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
# MAGIC   ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate,
# MAGIC   SUM(claim_amount) as total_amount,
# MAGIC   SUM(CASE WHEN is_fraud THEN claim_amount ELSE 0 END) as fraud_amount
# MAGIC FROM ${catalog}.${schema}.claims
# MAGIC GROUP BY DATE_TRUNC('month', claim_date)
# MAGIC ORDER BY claim_month DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: High-Risk Claims Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a comprehensive fraud risk view
# MAGIC CREATE OR REPLACE TEMP VIEW fraud_risk_dashboard AS
# MAGIC WITH claim_connections AS (
# MAGIC   SELECT 
# MAGIC     c.claim_id,
# MAGIC     COUNT(DISTINCT CASE WHEN cr.claim_id_1 = c.claim_id THEN cr.claim_id_2 ELSE cr.claim_id_1 END) as connection_count
# MAGIC   FROM ${catalog}.${schema}.claims c
# MAGIC   LEFT JOIN ${catalog}.${schema}.claim_relationships cr
# MAGIC     ON (c.claim_id = cr.claim_id_1 OR c.claim_id = cr.claim_id_2)
# MAGIC   GROUP BY c.claim_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   c.claim_id,
# MAGIC   c.policyholder_id,
# MAGIC   p.name as policyholder_name,
# MAGIC   c.claim_type,
# MAGIC   c.claim_date,
# MAGIC   c.claim_amount,
# MAGIC   c.claim_status,
# MAGIC   c.is_fraud,
# MAGIC   c.fraud_ring_id,
# MAGIC   COALESCE(cc.connection_count, 0) as network_connections,
# MAGIC   CASE 
# MAGIC     WHEN c.is_fraud THEN 'Confirmed Fraud'
# MAGIC     WHEN c.fraud_ring_id IS NOT NULL THEN 'Fraud Ring Member'
# MAGIC     WHEN COALESCE(cc.connection_count, 0) > 5 THEN 'High Network Connections'
# MAGIC     WHEN c.claim_amount > 50000 THEN 'High Value Claim'
# MAGIC     ELSE 'Normal'
# MAGIC   END as risk_category
# MAGIC FROM ${catalog}.${schema}.claims c
# MAGIC LEFT JOIN ${catalog}.${schema}.policyholders p ON c.policyholder_id = p.policyholder_id
# MAGIC LEFT JOIN claim_connections cc ON c.claim_id = cc.claim_id
# MAGIC ORDER BY 
# MAGIC   CASE risk_category
# MAGIC     WHEN 'Confirmed Fraud' THEN 1
# MAGIC     WHEN 'Fraud Ring Member' THEN 2
# MAGIC     WHEN 'High Network Connections' THEN 3
# MAGIC     WHEN 'High Value Claim' THEN 4
# MAGIC     ELSE 5
# MAGIC   END,
# MAGIC   c.claim_amount DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fraud_risk_dashboard
# MAGIC WHERE risk_category != 'Normal'
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Export Results for Further Analysis

# COMMAND ----------

# Save high-risk claims to a table
spark.sql("""
CREATE OR REPLACE TABLE ${catalog}.${schema}.high_risk_claims AS
SELECT * FROM fraud_risk_dashboard
WHERE risk_category != 'Normal'
""")

print(f"High-risk claims saved to {catalog}.{schema}.high_risk_claims table")

