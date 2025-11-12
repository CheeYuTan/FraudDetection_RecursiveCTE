# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Fraud Detection - Recursive Network Analysis
# MAGIC 
# MAGIC This notebook demonstrates recursive SQL capabilities in Databricks to detect fraud networks and connected claims.
# MAGIC 
# MAGIC **Requirements:**
# MAGIC - Databricks Runtime 17.0 or later (required for recursive CTE support)

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

# Check Databricks Runtime version
try:
    runtime_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    print(f"Databricks Runtime: {runtime_version}")
    import re
    version_match = re.search(r'(\d+)\.(\d+)', runtime_version)
    if version_match:
        major, minor = int(version_match.group(1)), int(version_match.group(2))
        if major < 17 or (major == 17 and minor < 0):
            print("⚠️  WARNING: This project requires Databricks Runtime 17.0 or later.")
            print("   Recursive CTEs are only available in Runtime 17.0+ and will not work in older versions.")
    else:
        print("⚠️  Unable to determine runtime version. Please ensure you're using Runtime 17.0+")
except Exception as e:
    print("⚠️  Could not check runtime version. Please ensure you're using Databricks Runtime 17.0+")

spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Stored Procedures (Optional but Recommended)
# MAGIC 
# MAGIC Create reusable stored procedures for fraud detection. These can be used as tools in agentic systems.
# MAGIC Based on patterns from: https://medium.com/dbsql-sme-engineering/graph-analytics-with-dbsql-stored-procedures-and-agents-5e53bad68032

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stored Procedure 1: Fraud Network BFS (Breadth-First Search)
# MAGIC CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.fraud_network_bfs(
# MAGIC   start_claim_id STRING,
# MAGIC   max_depth INT
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   claim_id STRING,
# MAGIC   policyholder_id STRING,
# MAGIC   claim_amount DOUBLE,
# MAGIC   is_fraud BOOLEAN,
# MAGIC   depth INT,
# MAGIC   path STRING,
# MAGIC   root_claim_id STRING
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC AS
# MAGIC $$
# MAGIC   WITH RECURSIVE fraud_network AS (
# MAGIC     SELECT 
# MAGIC       c.claim_id,
# MAGIC       c.policyholder_id,
# MAGIC       c.claim_amount,
# MAGIC       c.is_fraud,
# MAGIC       0 as depth,
# MAGIC       CAST(c.claim_id AS STRING) as path,
# MAGIC       c.claim_id as root_claim_id
# MAGIC     FROM ${catalog}.${schema}.claims c
# MAGIC     WHERE c.claim_id = start_claim_id
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 
# MAGIC       c.claim_id,
# MAGIC       c.policyholder_id,
# MAGIC       c.claim_amount,
# MAGIC       c.is_fraud,
# MAGIC       fn.depth + 1,
# MAGIC       CONCAT(fn.path, ' -> ', c.claim_id) as path,
# MAGIC       fn.root_claim_id
# MAGIC     FROM fraud_network fn
# MAGIC     INNER JOIN ${catalog}.${schema}.claim_relationships cr 
# MAGIC       ON (fn.claim_id = cr.claim_id_1 OR fn.claim_id = cr.claim_id_2)
# MAGIC     INNER JOIN ${catalog}.${schema}.claims c
# MAGIC       ON (c.claim_id = CASE 
# MAGIC           WHEN fn.claim_id = cr.claim_id_1 THEN cr.claim_id_2 
# MAGIC           ELSE cr.claim_id_1 
# MAGIC         END)
# MAGIC     WHERE fn.depth < max_depth
# MAGIC       AND c.claim_id != fn.claim_id
# MAGIC       AND fn.path NOT LIKE CONCAT('%', c.claim_id, '%')
# MAGIC   )
# MAGIC   SELECT * FROM fraud_network
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stored Procedure 2: Discover Fraud Networks
# MAGIC CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.discover_fraud_networks(
# MAGIC   max_depth INT DEFAULT 5,
# MAGIC   min_network_size INT DEFAULT 2
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   root_claim_id STRING,
# MAGIC   network_size BIGINT,
# MAGIC   total_network_amount DOUBLE,
# MAGIC   fraud_count BIGINT,
# MAGIC   max_depth INT,
# MAGIC   network_claims ARRAY<STRING>
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC AS
# MAGIC $$
# MAGIC   WITH RECURSIVE fraud_network AS (
# MAGIC     SELECT 
# MAGIC       c.claim_id,
# MAGIC       c.policyholder_id,
# MAGIC       c.claim_amount,
# MAGIC       c.is_fraud,
# MAGIC       0 as depth,
# MAGIC       CAST(c.claim_id AS STRING) as path,
# MAGIC       c.claim_id as root_claim_id
# MAGIC     FROM ${catalog}.${schema}.claims c
# MAGIC     WHERE c.is_fraud = true
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 
# MAGIC       c.claim_id,
# MAGIC       c.policyholder_id,
# MAGIC       c.claim_amount,
# MAGIC       c.is_fraud,
# MAGIC       fn.depth + 1,
# MAGIC       CONCAT(fn.path, ' -> ', c.claim_id) as path,
# MAGIC       fn.root_claim_id
# MAGIC     FROM fraud_network fn
# MAGIC     INNER JOIN ${catalog}.${schema}.claim_relationships cr 
# MAGIC       ON (fn.claim_id = cr.claim_id_1 OR fn.claim_id = cr.claim_id_2)
# MAGIC     INNER JOIN ${catalog}.${schema}.claims c
# MAGIC       ON (c.claim_id = CASE 
# MAGIC           WHEN fn.claim_id = cr.claim_id_1 THEN cr.claim_id_2 
# MAGIC           ELSE cr.claim_id_1 
# MAGIC         END)
# MAGIC     WHERE fn.depth < max_depth
# MAGIC       AND c.claim_id != fn.claim_id
# MAGIC       AND fn.path NOT LIKE CONCAT('%', c.claim_id, '%')
# MAGIC   )
# MAGIC   SELECT 
# MAGIC     root_claim_id,
# MAGIC     COUNT(DISTINCT claim_id) as network_size,
# MAGIC     SUM(claim_amount) as total_network_amount,
# MAGIC     SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
# MAGIC     MAX(depth) as max_depth,
# MAGIC     COLLECT_SET(claim_id) as network_claims
# MAGIC   FROM fraud_network
# MAGIC   GROUP BY root_claim_id
# MAGIC   HAVING network_size >= min_network_size
# MAGIC   ORDER BY network_size DESC, total_network_amount DESC
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stored Procedure 3: Get Claim Relationships (On-Demand)
# MAGIC -- Computes relationships for a specific claim without pre-generation
# MAGIC CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.get_claim_relationships(
# MAGIC   target_claim_id STRING
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   related_claim_id STRING,
# MAGIC   relationship_type STRING,
# MAGIC   strength DOUBLE
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC AS
# MAGIC $$
# MAGIC   WITH target_claim AS (
# MAGIC     SELECT 
# MAGIC       c.claim_id,
# MAGIC       c.policyholder_id,
# MAGIC       c.claim_type,
# MAGIC       c.claim_date,
# MAGIC       c.claim_amount,
# MAGIC       c.adjuster_id,
# MAGIC       p.address,
# MAGIC       p.phone
# MAGIC     FROM ${catalog}.${schema}.claims c
# MAGIC     INNER JOIN ${catalog}.${schema}.policyholders p ON c.policyholder_id = p.policyholder_id
# MAGIC     WHERE c.claim_id = target_claim_id
# MAGIC   ),
# MAGIC   related_claims AS (
# MAGIC     -- 1. Policyholder connections: Same address or phone
# MAGIC     SELECT DISTINCT
# MAGIC       c.claim_id as related_claim_id,
# MAGIC       'policyholder_connection' as relationship_type,
# MAGIC       0.8 as strength
# MAGIC     FROM ${catalog}.${schema}.claims c
# MAGIC     INNER JOIN ${catalog}.${schema}.policyholders p ON c.policyholder_id = p.policyholder_id
# MAGIC     CROSS JOIN target_claim tc
# MAGIC     WHERE c.claim_id != tc.claim_id
# MAGIC       AND (
# MAGIC         (p.address = tc.address AND p.address IS NOT NULL) OR
# MAGIC         (p.phone = tc.phone AND p.phone IS NOT NULL)
# MAGIC       )
# MAGIC     
# MAGIC     UNION
# MAGIC     
# MAGIC     -- 2. Temporal patterns: Same claim type, within 30 days, similar amounts
# MAGIC     SELECT DISTINCT
# MAGIC       c.claim_id as related_claim_id,
# MAGIC       'temporal_pattern' as relationship_type,
# MAGIC       0.6 as strength
# MAGIC     FROM ${catalog}.${schema}.claims c
# MAGIC     CROSS JOIN target_claim tc
# MAGIC     WHERE c.claim_id != tc.claim_id
# MAGIC       AND c.claim_type = tc.claim_type
# MAGIC       AND ABS(DATEDIFF(c.claim_date, tc.claim_date)) < 30
# MAGIC       AND ABS(c.claim_amount - tc.claim_amount) < tc.claim_amount * 0.3
# MAGIC     
# MAGIC     UNION
# MAGIC     
# MAGIC     -- 3. Service provider connections: Same adjuster, within 90 days
# MAGIC     SELECT DISTINCT
# MAGIC       c.claim_id as related_claim_id,
# MAGIC       'service_provider_connection' as relationship_type,
# MAGIC       0.7 as strength
# MAGIC     FROM ${catalog}.${schema}.claims c
# MAGIC     CROSS JOIN target_claim tc
# MAGIC     WHERE c.claim_id != tc.claim_id
# MAGIC       AND c.adjuster_id = tc.adjuster_id
# MAGIC       AND ABS(DATEDIFF(c.claim_date, tc.claim_date)) < 90
# MAGIC   )
# MAGIC   SELECT * FROM related_claims
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stored Procedure 4: Discover Fraud Networks with On-Demand Relationships
# MAGIC -- Works without pre-generated relationships table
# MAGIC CREATE OR REPLACE PROCEDURE ${catalog}.${schema}.discover_fraud_networks_ondemand(
# MAGIC   max_depth INT DEFAULT 5,
# MAGIC   min_network_size INT DEFAULT 2
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC   root_claim_id STRING,
# MAGIC   network_size BIGINT,
# MAGIC   total_network_amount DOUBLE,
# MAGIC   fraud_count BIGINT,
# MAGIC   max_depth INT
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC AS
# MAGIC $$
# MAGIC   WITH RECURSIVE fraud_network AS (
# MAGIC     SELECT 
# MAGIC       c.claim_id,
# MAGIC       c.policyholder_id,
# MAGIC       c.claim_amount,
# MAGIC       c.is_fraud,
# MAGIC       0 as depth,
# MAGIC       CAST(c.claim_id AS STRING) as path,
# MAGIC       c.claim_id as root_claim_id
# MAGIC     FROM ${catalog}.${schema}.claims c
# MAGIC     WHERE c.is_fraud = true
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 
# MAGIC       c2.claim_id,
# MAGIC       c2.policyholder_id,
# MAGIC       c2.claim_amount,
# MAGIC       c2.is_fraud,
# MAGIC       fn.depth + 1,
# MAGIC       CONCAT(fn.path, ' -> ', c2.claim_id) as path,
# MAGIC       fn.root_claim_id
# MAGIC     FROM fraud_network fn
# MAGIC     INNER JOIN ${catalog}.${schema}.claims c1 ON fn.claim_id = c1.claim_id
# MAGIC     INNER JOIN ${catalog}.${schema}.policyholders p1 ON c1.policyholder_id = p1.policyholder_id
# MAGIC     INNER JOIN ${catalog}.${schema}.claims c2 ON c2.claim_id != c1.claim_id
# MAGIC     INNER JOIN ${catalog}.${schema}.policyholders p2 ON c2.policyholder_id = p2.policyholder_id
# MAGIC     WHERE fn.depth < max_depth
# MAGIC       AND fn.path NOT LIKE CONCAT('%', c2.claim_id, '%')
# MAGIC       AND (
# MAGIC         (p1.address = p2.address AND p1.address IS NOT NULL) OR
# MAGIC         (p1.phone = p2.phone AND p1.phone IS NOT NULL) OR
# MAGIC         (c1.claim_type = c2.claim_type AND 
# MAGIC          ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 30 AND
# MAGIC          ABS(c1.claim_amount - c2.claim_amount) < c1.claim_amount * 0.3) OR
# MAGIC         (c1.adjuster_id = c2.adjuster_id AND 
# MAGIC          ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 90)
# MAGIC       )
# MAGIC   )
# MAGIC   SELECT 
# MAGIC     root_claim_id,
# MAGIC     COUNT(DISTINCT claim_id) as network_size,
# MAGIC     SUM(claim_amount) as total_network_amount,
# MAGIC     SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
# MAGIC     MAX(depth) as max_depth
# MAGIC   FROM fraud_network
# MAGIC   GROUP BY root_claim_id
# MAGIC   HAVING network_size >= min_network_size
# MAGIC   ORDER BY network_size DESC, total_network_amount DESC
# MAGIC $$;

# COMMAND ----------

print("✓ Stored procedures created successfully!")
print(f"  - {catalog}.{schema}.fraud_network_bfs")
print(f"  - {catalog}.{schema}.discover_fraud_networks")
print(f"  - {catalog}.{schema}.get_claim_relationships")
print(f"  - {catalog}.{schema}.discover_fraud_networks_ondemand")
print("\nYou can now use these stored procedures as reusable tools!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Recursive CTE to Find Fraud Networks
# MAGIC 
# MAGIC **Option A: Using Stored Procedure (Recommended)**
# MAGIC 
# MAGIC Use the stored procedure for cleaner, reusable code:
# MAGIC 
# MAGIC This recursive query finds all claims connected through relationships, building fraud networks.
# MAGIC 
# MAGIC **Note:** If you skipped relationship generation (`generate_relationships = false`), this query will use the pre-generated relationships table if it exists, or compute relationships on-demand. For large datasets, pre-generating relationships is recommended for better performance.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recursive CTE to find connected fraud networks
# MAGIC -- Uses pre-generated relationships if available, otherwise computes on-demand using UNION
# MAGIC WITH RECURSIVE fraud_network AS (
# MAGIC   -- Base case: Start with known fraudulent claims
# MAGIC   SELECT 
# MAGIC     c.claim_id,
# MAGIC     c.policyholder_id,
# MAGIC     c.claim_amount,
# MAGIC     c.is_fraud,
# MAGIC     0 as depth,
# MAGIC     CAST(c.claim_id AS STRING) as path,
# MAGIC     c.claim_id as root_claim_id
# MAGIC   FROM ${catalog}.${schema}.claims c
# MAGIC   WHERE c.is_fraud = true
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   -- Recursive case: Find connected claims
# MAGIC   -- Uses pre-generated relationships if available
# MAGIC   SELECT 
# MAGIC     c.claim_id,
# MAGIC     c.policyholder_id,
# MAGIC     c.claim_amount,
# MAGIC     c.is_fraud,
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
# MAGIC ## Step 5: Example - Get Relationships for a Specific Claim
# MAGIC 
# MAGIC Use the stored procedure to get relationships on-demand for any claim:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get relationships for a specific claim (on-demand computation)
# MAGIC -- First, let's pick a fraudulent claim to explore
# MAGIC SELECT claim_id 
# MAGIC FROM ${catalog}.${schema}.claims 
# MAGIC WHERE is_fraud = true 
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Replace 'CLM00000001' with an actual claim_id from above
# MAGIC -- This computes relationships on-demand without needing pre-generated relationships table
# MAGIC CALL ${catalog}.${schema}.get_claim_relationships('CLM00000001')
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Example - BFS from a Specific Claim
# MAGIC 
# MAGIC Use breadth-first search to explore a claim's network:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BFS from a specific claim (replace with actual claim_id)
# MAGIC CALL ${catalog}.${schema}.fraud_network_bfs(
# MAGIC   start_claim_id => 'CLM00000001',
# MAGIC   max_depth => 3
# MAGIC )
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Find Policyholder Networks (Shared Connections)
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
# MAGIC   -- Base case: Start with policyholders who have fraudulent claims
# MAGIC   SELECT DISTINCT
# MAGIC     c.policyholder_id,
# MAGIC     c.policyholder_id as root_policyholder,
# MAGIC     0 as depth,
# MAGIC     CAST(c.policyholder_id AS STRING) as path
# MAGIC   FROM ${catalog}.${schema}.claims c
# MAGIC   WHERE c.is_fraud = true
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
# MAGIC ## Step 8: Identify Suspicious Claim Patterns Using Recursion

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
# MAGIC ## Step 9: Create Fraud Risk Score Using Network Analysis

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
# MAGIC     (CASE WHEN COALESCE(cc.connection_count, 0) > 10 THEN 25 ELSE 0 END)
# MAGIC   ) as fraud_risk_score
# MAGIC FROM ${catalog}.${schema}.claims c
# MAGIC LEFT JOIN claim_connections cc ON c.claim_id = cc.claim_id
# MAGIC LEFT JOIN fraud_network_members fnm ON c.claim_id = fnm.claim_id
# MAGIC ORDER BY fraud_risk_score DESC, c.claim_amount DESC
# MAGIC LIMIT 50;

