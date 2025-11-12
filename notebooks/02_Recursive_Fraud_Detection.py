# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Fraud Detection - Recursive Network Analysis
# MAGIC 
# MAGIC This notebook demonstrates recursive SQL capabilities in Databricks to detect fraud networks and connected claims.

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
# MAGIC ## Step 2: Setup

# COMMAND ----------

spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Recursive CTE to Find Fraud Networks
# MAGIC 
# MAGIC This recursive query finds all claims connected through relationships, building fraud networks.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recursive CTE to find connected fraud networks
# MAGIC WITH RECURSIVE fraud_network AS (
# MAGIC   -- Base case: Start with known fraudulent claims
# MAGIC   SELECT 
# MAGIC     c.claim_id,
# MAGIC     c.policyholder_id,
# MAGIC     c.claim_amount,
# MAGIC     c.is_fraud,
# MAGIC     c.fraud_ring_id,
# MAGIC     0 as depth,
# MAGIC     CAST(c.claim_id AS STRING) as path,
# MAGIC     c.claim_id as root_claim_id
# MAGIC   FROM ${catalog}.${schema}.claims c
# MAGIC   WHERE c.is_fraud = true
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Recursive case: Find connected claims
# MAGIC   SELECT 
# MAGIC     c.claim_id,
# MAGIC     c.policyholder_id,
# MAGIC     c.claim_amount,
# MAGIC     c.is_fraud,
# MAGIC     c.fraud_ring_id,
# MAGIC     fn.depth + 1,
# MAGIC     CONCAT(fn.path, ' -> ', c.claim_id) as path,
# MAGIC     fn.root_claim_id
# MAGIC   FROM fraud_network fn
# MAGIC   INNER JOIN ${catalog}.${schema}.claim_relationships cr 
# MAGIC     ON (fn.claim_id = cr.claim_id_1 OR fn.claim_id = cr.claim_id_2)
# MAGIC   INNER JOIN ${catalog}.${schema}.claims c
# MAGIC     ON (c.claim_id = CASE 
# MAGIC         WHEN fn.claim_id = cr.claim_id_1 THEN cr.claim_id_2 
# MAGIC         ELSE cr.claim_id_1 
# MAGIC       END)
# MAGIC   WHERE fn.depth < 5  -- Limit recursion depth
# MAGIC     AND c.claim_id != fn.claim_id
# MAGIC     AND fn.path NOT LIKE CONCAT('%', c.claim_id, '%')  -- Prevent cycles
# MAGIC )
# MAGIC SELECT 
# MAGIC   root_claim_id,
# MAGIC   COUNT(DISTINCT claim_id) as network_size,
# MAGIC   SUM(claim_amount) as total_network_amount,
# MAGIC   SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
# MAGIC   MAX(depth) as max_depth,
# MAGIC   COLLECT_SET(claim_id) as network_claims
# MAGIC FROM fraud_network
# MAGIC GROUP BY root_claim_id
# MAGIC HAVING network_size > 1
# MAGIC ORDER BY network_size DESC, total_network_amount DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Find Policyholder Networks (Shared Connections)
# MAGIC 
# MAGIC Recursive query to find policyholders connected through shared claims or relationships.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a policyholder relationship graph
# MAGIC WITH policyholder_connections AS (
# MAGIC   -- Direct connections: policyholders with shared claim relationships
# MAGIC   SELECT DISTINCT
# MAGIC     c1.policyholder_id as ph1,
# MAGIC     c2.policyholder_id as ph2,
# MAGIC     1 as connection_strength
# MAGIC   FROM ${catalog}.${schema}.claims c1
# MAGIC   INNER JOIN ${catalog}.${schema}.claim_relationships cr 
# MAGIC     ON c1.claim_id = cr.claim_id_1
# MAGIC   INNER JOIN ${catalog}.${schema}.claims c2
# MAGIC     ON c2.claim_id = cr.claim_id_2
# MAGIC   WHERE c1.policyholder_id != c2.policyholder_id
# MAGIC   
# MAGIC   UNION
# MAGIC   
# MAGIC   -- Indirect connections: policyholders with similar patterns
# MAGIC   SELECT DISTINCT
# MAGIC     c1.policyholder_id as ph1,
# MAGIC     c2.policyholder_id as ph2,
# MAGIC     0.5 as connection_strength
# MAGIC   FROM ${catalog}.${schema}.claims c1
# MAGIC   INNER JOIN ${catalog}.${schema}.claims c2
# MAGIC     ON c1.claim_type = c2.claim_type
# MAGIC     AND c1.policyholder_id != c2.policyholder_id
# MAGIC     AND ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 30
# MAGIC     AND ABS(c1.claim_amount - c2.claim_amount) < c1.claim_amount * 0.2
# MAGIC ),
# MAGIC 
# MAGIC RECURSIVE policyholder_network AS (
# MAGIC   -- Base case: Start with known fraud ring members
# MAGIC   SELECT 
# MAGIC     ph.policyholder_id,
# MAGIC     ph.policyholder_id as root_policyholder,
# MAGIC     0 as depth,
# MAGIC     CAST(ph.policyholder_id AS STRING) as path
# MAGIC   FROM ${catalog}.${schema}.policyholders ph
# MAGIC   WHERE ph.is_fraud_ring_member = true
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Recursive case: Find connected policyholders
# MAGIC   SELECT 
# MAGIC     pc.ph2 as policyholder_id,
# MAGIC     pn.root_policyholder,
# MAGIC     pn.depth + 1,
# MAGIC     CONCAT(pn.path, ' -> ', pc.ph2)
# MAGIC   FROM policyholder_network pn
# MAGIC   INNER JOIN policyholder_connections pc
# MAGIC     ON pn.policyholder_id = pc.ph1
# MAGIC   WHERE pn.depth < 3
# MAGIC     AND pc.ph2 != pn.policyholder_id
# MAGIC     AND pn.path NOT LIKE CONCAT('%', pc.ph2, '%')
# MAGIC )
# MAGIC SELECT 
# MAGIC   root_policyholder,
# MAGIC   COUNT(DISTINCT policyholder_id) as network_size,
# MAGIC   COLLECT_SET(policyholder_id) as network_members
# MAGIC FROM policyholder_network
# MAGIC GROUP BY root_policyholder
# MAGIC HAVING network_size > 1
# MAGIC ORDER BY network_size DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Identify Suspicious Claim Patterns Using Recursion

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recursive query to find chains of suspicious claims
# MAGIC WITH RECURSIVE suspicious_chain AS (
# MAGIC   -- Base case: High-value claims or claims with multiple relationships
# MAGIC   SELECT 
# MAGIC     c.claim_id,
# MAGIC     c.policyholder_id,
# MAGIC     c.claim_amount,
# MAGIC     c.claim_date,
# MAGIC     0 as chain_length,
# MAGIC     CAST(c.claim_id AS STRING) as chain_path,
# MAGIC     c.claim_id as chain_start
# MAGIC   FROM ${catalog}.${schema}.claims c
# MAGIC   WHERE c.claim_amount > 50000
# MAGIC     OR (
# MAGIC       SELECT COUNT(*) 
# MAGIC       FROM ${catalog}.${schema}.claim_relationships cr 
# MAGIC       WHERE cr.claim_id_1 = c.claim_id OR cr.claim_id_2 = c.claim_id
# MAGIC     ) > 3
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Recursive case: Find connected suspicious claims
# MAGIC   SELECT 
# MAGIC     c.claim_id,
# MAGIC     c.policyholder_id,
# MAGIC     c.claim_amount,
# MAGIC     c.claim_date,
# MAGIC     sc.chain_length + 1,
# MAGIC     CONCAT(sc.chain_path, ' -> ', c.claim_id),
# MAGIC     sc.chain_start
# MAGIC   FROM suspicious_chain sc
# MAGIC   INNER JOIN ${catalog}.${schema}.claim_relationships cr
# MAGIC     ON (sc.claim_id = cr.claim_id_1 OR sc.claim_id = cr.claim_id_2)
# MAGIC   INNER JOIN ${catalog}.${schema}.claims c
# MAGIC     ON (c.claim_id = CASE 
# MAGIC         WHEN sc.claim_id = cr.claim_id_1 THEN cr.claim_id_2 
# MAGIC         ELSE cr.claim_id_1 
# MAGIC       END)
# MAGIC   WHERE sc.chain_length < 4
# MAGIC     AND c.claim_id != sc.claim_id
# MAGIC     AND sc.chain_path NOT LIKE CONCAT('%', c.claim_id, '%')
# MAGIC     AND (
# MAGIC       c.claim_amount > 30000
# MAGIC       OR ABS(DATEDIFF(c.claim_date, sc.claim_date)) < 90
# MAGIC     )
# MAGIC )
# MAGIC SELECT 
# MAGIC   chain_start,
# MAGIC   COUNT(DISTINCT claim_id) as chain_size,
# MAGIC   SUM(claim_amount) as total_chain_amount,
# MAGIC   MAX(chain_length) as max_chain_length,
# MAGIC   COLLECT_SET(claim_id) as chain_claims,
# MAGIC   MAX(chain_path) as full_chain_path
# MAGIC FROM suspicious_chain
# MAGIC GROUP BY chain_start
# MAGIC HAVING chain_size >= 3
# MAGIC ORDER BY total_chain_amount DESC, chain_size DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Fraud Risk Score Using Network Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate fraud risk scores based on network connections
# MAGIC WITH claim_connections AS (
# MAGIC   SELECT 
# MAGIC     c.claim_id,
# MAGIC     COUNT(DISTINCT cr.claim_id_1) + COUNT(DISTINCT cr.claim_id_2) - 1 as connection_count
# MAGIC   FROM ${catalog}.${schema}.claims c
# MAGIC   LEFT JOIN ${catalog}.${schema}.claim_relationships cr
# MAGIC     ON (c.claim_id = cr.claim_id_1 OR c.claim_id = cr.claim_id_2)
# MAGIC   GROUP BY c.claim_id
# MAGIC ),
# MAGIC 
# MAGIC fraud_network_members AS (
# MAGIC   SELECT DISTINCT claim_id
# MAGIC   FROM (
# MAGIC     WITH RECURSIVE fraud_network AS (
# MAGIC       SELECT 
# MAGIC         c.claim_id,
# MAGIC         0 as depth,
# MAGIC         CAST(c.claim_id AS STRING) as path
# MAGIC       FROM ${catalog}.${schema}.claims c
# MAGIC       WHERE c.is_fraud = true
# MAGIC       
# MAGIC       UNION ALL
# MAGIC       
# MAGIC       SELECT 
# MAGIC         c.claim_id,
# MAGIC         fn.depth + 1,
# MAGIC         CONCAT(fn.path, ' -> ', c.claim_id)
# MAGIC       FROM fraud_network fn
# MAGIC       INNER JOIN ${catalog}.${schema}.claim_relationships cr 
# MAGIC         ON (fn.claim_id = cr.claim_id_1 OR fn.claim_id = cr.claim_id_2)
# MAGIC       INNER JOIN ${catalog}.${schema}.claims c
# MAGIC         ON (c.claim_id = CASE 
# MAGIC             WHEN fn.claim_id = cr.claim_id_1 THEN cr.claim_id_2 
# MAGIC             ELSE cr.claim_id_1 
# MAGIC           END)
# MAGIC       WHERE fn.depth < 3
# MAGIC         AND c.claim_id != fn.claim_id
# MAGIC         AND fn.path NOT LIKE CONCAT('%', c.claim_id, '%')
# MAGIC     )
# MAGIC     SELECT claim_id FROM fraud_network
# MAGIC   )
# MAGIC )
# MAGIC 
# MAGIC SELECT 
# MAGIC   c.claim_id,
# MAGIC   c.policyholder_id,
# MAGIC   c.claim_type,
# MAGIC   c.claim_amount,
# MAGIC   c.claim_date,
# MAGIC   c.is_fraud,
# MAGIC   COALESCE(cc.connection_count, 0) as connection_count,
# MAGIC   CASE WHEN fnm.claim_id IS NOT NULL THEN 1 ELSE 0 END as in_fraud_network,
# MAGIC   -- Calculate risk score (0-100)
# MAGIC   LEAST(100, 
# MAGIC     (CASE WHEN c.is_fraud THEN 50 ELSE 0 END) +
# MAGIC     (CASE WHEN fnm.claim_id IS NOT NULL THEN 30 ELSE 0 END) +
# MAGIC     (CASE WHEN COALESCE(cc.connection_count, 0) > 5 THEN 20 ELSE 0 END) +
# MAGIC     (CASE WHEN c.claim_amount > 50000 THEN 15 ELSE 0 END) +
# MAGIC     (CASE WHEN c.fraud_ring_id IS NOT NULL THEN 25 ELSE 0 END)
# MAGIC   ) as fraud_risk_score
# MAGIC FROM ${catalog}.${schema}.claims c
# MAGIC LEFT JOIN claim_connections cc ON c.claim_id = cc.claim_id
# MAGIC LEFT JOIN fraud_network_members fnm ON c.claim_id = fnm.claim_id
# MAGIC ORDER BY fraud_risk_score DESC, c.claim_amount DESC
# MAGIC LIMIT 50;

