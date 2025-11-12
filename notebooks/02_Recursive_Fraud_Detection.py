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
dbutils.widgets.text("schema", "fraud_detection_demo", "Schema/Database Name")

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

# Use catalog and schema separately
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Stored Procedures (Optional but Recommended)
# MAGIC 
# MAGIC Create reusable stored procedures for fraud detection. These can be used as tools in agentic systems.

# COMMAND ----------

# Stored Procedure 1: Fraud Network BFS (Breadth-First Search)
# Uses on-demand relationship computation
spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.fraud_network_bfs(
  start_claim_id STRING,
  max_depth INT
)
RETURNS TABLE (
  claim_id STRING,
  policyholder_id STRING,
  claim_amount DOUBLE,
  is_fraud BOOLEAN,
  depth INT,
  path STRING,
  root_claim_id STRING
)
LANGUAGE SQL
AS
$$
  WITH RECURSIVE fraud_network AS (
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_amount,
      c.is_fraud,
      0 as depth,
      CAST(c.claim_id AS STRING) as path,
      c.claim_id as root_claim_id
    FROM {catalog}.{schema}.claims c
    WHERE c.claim_id = start_claim_id
    
    UNION ALL
    
    SELECT 
      c2.claim_id,
      c2.policyholder_id,
      c2.claim_amount,
      c2.is_fraud,
      fn.depth + 1,
      CONCAT(fn.path, ' -> ', c2.claim_id) as path,
      fn.root_claim_id
    FROM fraud_network fn
    INNER JOIN {catalog}.{schema}.claims c1 ON fn.claim_id = c1.claim_id
    INNER JOIN {catalog}.{schema}.policyholders p1 ON c1.policyholder_id = p1.policyholder_id
    INNER JOIN {catalog}.{schema}.claims c2 ON c2.claim_id != c1.claim_id
    INNER JOIN {catalog}.{schema}.policyholders p2 ON c2.policyholder_id = p2.policyholder_id
    WHERE fn.depth < max_depth
      AND fn.path NOT LIKE CONCAT('%', c2.claim_id, '%')
      AND (
        (p1.address = p2.address AND p1.address IS NOT NULL) OR
        (p1.phone = p2.phone AND p1.phone IS NOT NULL) OR
        (c1.claim_type = c2.claim_type AND 
         ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 30 AND
         ABS(c1.claim_amount - c2.claim_amount) < c1.claim_amount * 0.3) OR
        (c1.adjuster_id = c2.adjuster_id AND 
         ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 90)
      )
  )
  SELECT * FROM fraud_network
$$
""")

# COMMAND ----------

# Stored Procedure 2: Discover Fraud Networks (On-Demand Relationships)
spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.discover_fraud_networks(
  max_depth INT DEFAULT 5,
  min_network_size INT DEFAULT 2
)
RETURNS TABLE (
  root_claim_id STRING,
  network_size BIGINT,
  total_network_amount DOUBLE,
  fraud_count BIGINT,
  max_depth INT,
  network_claims ARRAY<STRING>
)
LANGUAGE SQL
AS
$$
  WITH RECURSIVE fraud_network AS (
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_amount,
      c.is_fraud,
      0 as depth,
      CAST(c.claim_id AS STRING) as path,
      c.claim_id as root_claim_id
    FROM {catalog}.{schema}.claims c
    WHERE c.is_fraud = true
    
    UNION ALL
    
    SELECT 
      c2.claim_id,
      c2.policyholder_id,
      c2.claim_amount,
      c2.is_fraud,
      fn.depth + 1,
      CONCAT(fn.path, ' -> ', c2.claim_id) as path,
      fn.root_claim_id
    FROM fraud_network fn
    INNER JOIN {catalog}.{schema}.claims c1 ON fn.claim_id = c1.claim_id
    INNER JOIN {catalog}.{schema}.policyholders p1 ON c1.policyholder_id = p1.policyholder_id
    INNER JOIN {catalog}.{schema}.claims c2 ON c2.claim_id != c1.claim_id
    INNER JOIN {catalog}.{schema}.policyholders p2 ON c2.policyholder_id = p2.policyholder_id
    WHERE fn.depth < max_depth
      AND fn.path NOT LIKE CONCAT('%', c2.claim_id, '%')
      AND (
        (p1.address = p2.address AND p1.address IS NOT NULL) OR
        (p1.phone = p2.phone AND p1.phone IS NOT NULL) OR
        (c1.claim_type = c2.claim_type AND 
         ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 30 AND
         ABS(c1.claim_amount - c2.claim_amount) < c1.claim_amount * 0.3) OR
        (c1.adjuster_id = c2.adjuster_id AND 
         ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 90)
      )
  )
  SELECT 
    root_claim_id,
    COUNT(DISTINCT claim_id) as network_size,
    SUM(claim_amount) as total_network_amount,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
    MAX(depth) as max_depth,
    COLLECT_SET(claim_id) as network_claims
  FROM fraud_network
  GROUP BY root_claim_id
  HAVING network_size >= min_network_size
  ORDER BY network_size DESC, total_network_amount DESC
$$
""")

# COMMAND ----------

# Stored Procedure 3: Get Claim Relationships (On-Demand)
# Computes relationships for a specific claim without pre-generation
spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.get_claim_relationships(
  target_claim_id STRING
)
RETURNS TABLE (
  related_claim_id STRING,
  relationship_type STRING,
  strength DOUBLE
)
LANGUAGE SQL
AS
$$
  WITH target_claim AS (
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_type,
      c.claim_date,
      c.claim_amount,
      c.adjuster_id,
      p.address,
      p.phone
    FROM {catalog}.{schema}.claims c
    INNER JOIN {catalog}.{schema}.policyholders p ON c.policyholder_id = p.policyholder_id
    WHERE c.claim_id = target_claim_id
  ),
  related_claims AS (
    -- 1. Policyholder connections: Same address or phone
    SELECT DISTINCT
      c.claim_id as related_claim_id,
      'policyholder_connection' as relationship_type,
      0.8 as strength
    FROM {catalog}.{schema}.claims c
    INNER JOIN {catalog}.{schema}.policyholders p ON c.policyholder_id = p.policyholder_id
    CROSS JOIN target_claim tc
    WHERE c.claim_id != tc.claim_id
      AND (
        (p.address = tc.address AND p.address IS NOT NULL) OR
        (p.phone = tc.phone AND p.phone IS NOT NULL)
      )
    
    UNION
    
    -- 2. Temporal patterns: Same claim type, within 30 days, similar amounts
    SELECT DISTINCT
      c.claim_id as related_claim_id,
      'temporal_pattern' as relationship_type,
      0.6 as strength
    FROM {catalog}.{schema}.claims c
    CROSS JOIN target_claim tc
    WHERE c.claim_id != tc.claim_id
      AND c.claim_type = tc.claim_type
      AND ABS(DATEDIFF(c.claim_date, tc.claim_date)) < 30
      AND ABS(c.claim_amount - tc.claim_amount) < tc.claim_amount * 0.3
    
    UNION
    
    -- 3. Service provider connections: Same adjuster, within 90 days
    SELECT DISTINCT
      c.claim_id as related_claim_id,
      'service_provider_connection' as relationship_type,
      0.7 as strength
    FROM {catalog}.{schema}.claims c
    CROSS JOIN target_claim tc
    WHERE c.claim_id != tc.claim_id
      AND c.adjuster_id = tc.adjuster_id
      AND ABS(DATEDIFF(c.claim_date, tc.claim_date)) < 90
  )
  SELECT * FROM related_claims
$$
""")

# COMMAND ----------

# Stored Procedure 4: Discover Fraud Networks with On-Demand Relationships
# Works without pre-generated relationships table (production approach)
spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.discover_fraud_networks_ondemand(
  max_depth INT DEFAULT 5,
  min_network_size INT DEFAULT 2
)
RETURNS TABLE (
  root_claim_id STRING,
  network_size BIGINT,
  total_network_amount DOUBLE,
  fraud_count BIGINT,
  max_depth INT
)
LANGUAGE SQL
AS
$$
  WITH RECURSIVE fraud_network AS (
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_amount,
      c.is_fraud,
      0 as depth,
      CAST(c.claim_id AS STRING) as path,
      c.claim_id as root_claim_id
    FROM {catalog}.{schema}.claims c
    WHERE c.is_fraud = true
    
    UNION ALL
    
    SELECT 
      c2.claim_id,
      c2.policyholder_id,
      c2.claim_amount,
      c2.is_fraud,
      fn.depth + 1,
      CONCAT(fn.path, ' -> ', c2.claim_id) as path,
      fn.root_claim_id
    FROM fraud_network fn
    INNER JOIN {catalog}.{schema}.claims c1 ON fn.claim_id = c1.claim_id
    INNER JOIN {catalog}.{schema}.policyholders p1 ON c1.policyholder_id = p1.policyholder_id
    INNER JOIN {catalog}.{schema}.claims c2 ON c2.claim_id != c1.claim_id
    INNER JOIN {catalog}.{schema}.policyholders p2 ON c2.policyholder_id = p2.policyholder_id
    WHERE fn.depth < max_depth
      AND fn.path NOT LIKE CONCAT('%', c2.claim_id, '%')
      AND (
        (p1.address = p2.address AND p1.address IS NOT NULL) OR
        (p1.phone = p2.phone AND p1.phone IS NOT NULL) OR
        (c1.claim_type = c2.claim_type AND 
         ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 30 AND
         ABS(c1.claim_amount - c2.claim_amount) < c1.claim_amount * 0.3) OR
        (c1.adjuster_id = c2.adjuster_id AND 
         ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 90)
      )
  )
  SELECT 
    root_claim_id,
    COUNT(DISTINCT claim_id) as network_size,
    SUM(claim_amount) as total_network_amount,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
    MAX(depth) as max_depth
  FROM fraud_network
  GROUP BY root_claim_id
  HAVING network_size >= min_network_size
  ORDER BY network_size DESC, total_network_amount DESC
$$
""")

# COMMAND ----------

print("✓ Stored procedures created successfully!")
print(f"  - {catalog}.{schema}.fraud_network_bfs")
print(f"  - {catalog}.{schema}.discover_fraud_networks")
print(f"  - {catalog}.{schema}.get_claim_relationships")
print(f"  - {catalog}.{schema}.discover_fraud_networks_ondemand")
print("\nYou can now use these stored procedures as reusable tools!")
print("Note: All procedures use on-demand relationship computation (production approach)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Recursive CTE to Find Fraud Networks
# MAGIC 
# MAGIC This recursive query finds all claims connected through relationships, building fraud networks.
# MAGIC 
# MAGIC **Option A: Using Stored Procedure (Recommended)**
# MAGIC 
# MAGIC Use the stored procedure for cleaner, reusable code:

# COMMAND ----------

# Use stored procedure to discover fraud networks
# Uses on-demand relationship computation
result_df = spark.sql(f"""
CALL {catalog}.{schema}.discover_fraud_networks(
  max_depth => 5,
  min_network_size => 2
)
""")
result_df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Option B: Using Stored Procedure with On-Demand Relationships (Same as Option A)**
# MAGIC 
# MAGIC Both stored procedures now use on-demand relationship computation:

# COMMAND ----------

# Use stored procedure with on-demand relationship computation
# No pre-generated relationships table needed
result_df = spark.sql(f"""
CALL {catalog}.{schema}.discover_fraud_networks_ondemand(
  max_depth => 5,
  min_network_size => 2
)
""")
result_df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Example - Get Relationships for a Specific Claim
# MAGIC 
# MAGIC Use the stored procedure to get relationships on-demand for any claim:

# COMMAND ----------

# Get relationships for a specific claim (on-demand computation)
# First, let's pick a fraudulent claim to explore
sample_claim = spark.sql(f"""
SELECT claim_id 
FROM {catalog}.{schema}.claims 
WHERE is_fraud = true 
LIMIT 1
""").collect()
if sample_claim:
    sample_claim_id = sample_claim[0]['claim_id']
    print(f"Sample fraudulent claim: {sample_claim_id}")
else:
    sample_claim_id = 'CLM00000001'
    print(f"No fraudulent claims found, using example: {sample_claim_id}")

# COMMAND ----------

# Get relationships for the sample claim (on-demand computation)
# This computes relationships on-demand without needing pre-generated relationships table
relationships_df = spark.sql(f"""
CALL {catalog}.{schema}.get_claim_relationships('{sample_claim_id}')
""")
relationships_df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Example - BFS from a Specific Claim
# MAGIC 
# MAGIC Use breadth-first search to explore a claim's network:

# COMMAND ----------

# BFS from a specific claim
bfs_result = spark.sql(f"""
CALL {catalog}.{schema}.fraud_network_bfs(
  start_claim_id => '{sample_claim_id}',
  max_depth => 3
)
""")
bfs_result.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Find Policyholder Networks (Shared Connections)
# MAGIC 
# MAGIC Recursive query to find policyholders connected through shared claims or relationships.

# COMMAND ----------

# Create a policyholder relationship graph using on-demand relationships
policyholder_network_df = spark.sql(f"""
WITH policyholder_connections AS (
  -- Direct connections: policyholders with shared attributes (address, phone)
  SELECT DISTINCT
    c1.policyholder_id as ph1,
    c2.policyholder_id as ph2,
    1 as connection_strength
  FROM {catalog}.{schema}.claims c1
  INNER JOIN {catalog}.{schema}.policyholders p1 ON c1.policyholder_id = p1.policyholder_id
  INNER JOIN {catalog}.{schema}.claims c2 ON c2.claim_id != c1.claim_id
  INNER JOIN {catalog}.{schema}.policyholders p2 ON c2.policyholder_id = p2.policyholder_id
  WHERE c1.policyholder_id != c2.policyholder_id
    AND (
      (p1.address = p2.address AND p1.address IS NOT NULL) OR
      (p1.phone = p2.phone AND p1.phone IS NOT NULL)
    )
  
  UNION
  
  -- Indirect connections: policyholders with similar patterns
  SELECT DISTINCT
    c1.policyholder_id as ph1,
    c2.policyholder_id as ph2,
    0.5 as connection_strength
  FROM {catalog}.{schema}.claims c1
  INNER JOIN {catalog}.{schema}.claims c2
    ON c1.claim_type = c2.claim_type
    AND c1.policyholder_id != c2.policyholder_id
    AND ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 30
    AND ABS(c1.claim_amount - c2.claim_amount) < c1.claim_amount * 0.2
),

RECURSIVE policyholder_network AS (
  -- Base case: Start with policyholders who have fraudulent claims
  SELECT DISTINCT
    c.policyholder_id,
    c.policyholder_id as root_policyholder,
    0 as depth,
    CAST(c.policyholder_id AS STRING) as path
  FROM {catalog}.{schema}.claims c
  WHERE c.is_fraud = true
  
  UNION ALL
  
  -- Recursive case: Find connected policyholders
  SELECT 
    pc.ph2 as policyholder_id,
    pn.root_policyholder,
    pn.depth + 1,
    CONCAT(pn.path, ' -> ', pc.ph2)
  FROM policyholder_network pn
  INNER JOIN policyholder_connections pc
    ON pn.policyholder_id = pc.ph1
  WHERE pn.depth < 3
    AND pc.ph2 != pn.policyholder_id
    AND pn.path NOT LIKE CONCAT('%', pc.ph2, '%')
)
SELECT 
  root_policyholder,
  COUNT(DISTINCT policyholder_id) as network_size,
  COLLECT_SET(policyholder_id) as network_members
FROM policyholder_network
GROUP BY root_policyholder
HAVING network_size > 1
ORDER BY network_size DESC
""")
policyholder_network_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Identify Suspicious Claim Patterns Using Recursion

# COMMAND ----------

# Recursive query to find chains of suspicious claims using on-demand relationships
suspicious_chains_df = spark.sql(f"""
WITH RECURSIVE suspicious_chain AS (
  -- Base case: High-value claims
  SELECT 
    c.claim_id,
    c.policyholder_id,
    c.claim_amount,
    c.claim_date,
    0 as chain_length,
    CAST(c.claim_id AS STRING) as chain_path,
    c.claim_id as chain_start
  FROM {catalog}.{schema}.claims c
  WHERE c.claim_amount > 50000
  
  UNION ALL
  
  -- Recursive case: Find connected suspicious claims using on-demand relationships
  SELECT 
    c2.claim_id,
    c2.policyholder_id,
    c2.claim_amount,
    c2.claim_date,
    sc.chain_length + 1,
    CONCAT(sc.chain_path, ' -> ', c2.claim_id),
    sc.chain_start
  FROM suspicious_chain sc
  INNER JOIN {catalog}.{schema}.claims c1 ON sc.claim_id = c1.claim_id
  INNER JOIN {catalog}.{schema}.policyholders p1 ON c1.policyholder_id = p1.policyholder_id
  INNER JOIN {catalog}.{schema}.claims c2 ON c2.claim_id != c1.claim_id
  INNER JOIN {catalog}.{schema}.policyholders p2 ON c2.policyholder_id = p2.policyholder_id
  WHERE sc.chain_length < 4
    AND sc.chain_path NOT LIKE CONCAT('%', c2.claim_id, '%')
    AND (
      (p1.address = p2.address AND p1.address IS NOT NULL) OR
      (p1.phone = p2.phone AND p1.phone IS NOT NULL) OR
      (c1.claim_type = c2.claim_type AND 
       ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 30 AND
       ABS(c1.claim_amount - c2.claim_amount) < c1.claim_amount * 0.3) OR
      (c1.adjuster_id = c2.adjuster_id AND 
       ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 90)
    )
    AND (
      c2.claim_amount > 30000
      OR ABS(DATEDIFF(c2.claim_date, sc.claim_date)) < 90
    )
)
SELECT 
  chain_start,
  COUNT(DISTINCT claim_id) as chain_size,
  SUM(claim_amount) as total_chain_amount,
  MAX(chain_length) as max_chain_length,
  COLLECT_SET(claim_id) as chain_claims,
  MAX(chain_path) as full_chain_path
FROM suspicious_chain
GROUP BY chain_start
HAVING chain_size >= 3
ORDER BY total_chain_amount DESC, chain_size DESC
LIMIT 15
""")
suspicious_chains_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create Fraud Risk Score Using Network Analysis

# COMMAND ----------

# Calculate fraud risk scores based on network connections using on-demand relationships
risk_scores_df = spark.sql(f"""
WITH claim_connections AS (
  -- Count connections using on-demand relationship computation
  SELECT 
    c1.claim_id,
    COUNT(DISTINCT c2.claim_id) as connection_count
  FROM {catalog}.{schema}.claims c1
  INNER JOIN {catalog}.{schema}.policyholders p1 ON c1.policyholder_id = p1.policyholder_id
  LEFT JOIN {catalog}.{schema}.claims c2 ON c2.claim_id != c1.claim_id
  LEFT JOIN {catalog}.{schema}.policyholders p2 ON c2.policyholder_id = p2.policyholder_id
  WHERE (
    (p1.address = p2.address AND p1.address IS NOT NULL) OR
    (p1.phone = p2.phone AND p1.phone IS NOT NULL) OR
    (c1.claim_type = c2.claim_type AND 
     ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 30 AND
     ABS(c1.claim_amount - c2.claim_amount) < c1.claim_amount * 0.3) OR
    (c1.adjuster_id = c2.adjuster_id AND 
     ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 90)
  )
  GROUP BY c1.claim_id
),

fraud_network_members AS (
  SELECT DISTINCT claim_id
  FROM (
    WITH RECURSIVE fraud_network AS (
      SELECT 
        c.claim_id,
        0 as depth,
        CAST(c.claim_id AS STRING) as path
      FROM {catalog}.{schema}.claims c
      WHERE c.is_fraud = true
      
      UNION ALL
      
      SELECT 
        c2.claim_id,
        fn.depth + 1,
        CONCAT(fn.path, ' -> ', c2.claim_id)
      FROM fraud_network fn
      INNER JOIN {catalog}.{schema}.claims c1 ON fn.claim_id = c1.claim_id
      INNER JOIN {catalog}.{schema}.policyholders p1 ON c1.policyholder_id = p1.policyholder_id
      INNER JOIN {catalog}.{schema}.claims c2 ON c2.claim_id != c1.claim_id
      INNER JOIN {catalog}.{schema}.policyholders p2 ON c2.policyholder_id = p2.policyholder_id
      WHERE fn.depth < 3
        AND fn.path NOT LIKE CONCAT('%', c2.claim_id, '%')
        AND (
          (p1.address = p2.address AND p1.address IS NOT NULL) OR
          (p1.phone = p2.phone AND p1.phone IS NOT NULL) OR
          (c1.claim_type = c2.claim_type AND 
           ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 30 AND
           ABS(c1.claim_amount - c2.claim_amount) < c1.claim_amount * 0.3) OR
          (c1.adjuster_id = c2.adjuster_id AND 
           ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 90)
        )
    )
    SELECT claim_id FROM fraud_network
  )
)

SELECT 
  c.claim_id,
  c.policyholder_id,
  c.claim_type,
  c.claim_amount,
  c.claim_date,
  c.is_fraud,
  COALESCE(cc.connection_count, 0) as connection_count,
  CASE WHEN fnm.claim_id IS NOT NULL THEN 1 ELSE 0 END as in_fraud_network,
  -- Calculate risk score (0-100)
  LEAST(100, 
    (CASE WHEN c.is_fraud THEN 50 ELSE 0 END) +
    (CASE WHEN fnm.claim_id IS NOT NULL THEN 30 ELSE 0 END) +
    (CASE WHEN COALESCE(cc.connection_count, 0) > 5 THEN 20 ELSE 0 END) +
    (CASE WHEN c.claim_amount > 50000 THEN 15 ELSE 0 END) +
    (CASE WHEN COALESCE(cc.connection_count, 0) > 10 THEN 25 ELSE 0 END)
  ) as fraud_risk_score
FROM {catalog}.{schema}.claims c
LEFT JOIN claim_connections cc ON c.claim_id = cc.claim_id
LEFT JOIN fraud_network_members fnm ON c.claim_id = fnm.claim_id
ORDER BY fraud_risk_score DESC, c.claim_amount DESC
LIMIT 50
""")
risk_scores_df.show(50, truncate=False)

