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
dbutils.widgets.text("catalog", "dbdemos_steventan", "Catalog Name")
dbutils.widgets.text("schema", "frauddetection_recursivecte", "Schema/Database Name")

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
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
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
  SELECT * FROM fraud_network;
END
""")

# COMMAND ----------

# Stored Procedure 2: Discover Fraud Network for Specific Claim (Optimized)
spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.discover_fraud_network_for_claim(
  start_claim_id STRING,
  max_depth INT DEFAULT 3
)
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Discover the fraud network starting from a specific claim ID (optimized for performance)'
AS
BEGIN
  WITH RECURSIVE fraud_network AS (
    -- Base case: Start from the given claim
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_amount,
      c.is_fraud,
      0 as depth,
      CAST(c.claim_id AS STRING) as path
    FROM {catalog}.{schema}.claims c
    WHERE c.claim_id = start_claim_id
    
    UNION ALL
    
    -- Recursive case: Find connected claims (optimized with direct joins)
    SELECT DISTINCT
      c2.claim_id,
      c2.policyholder_id,
      c2.claim_amount,
      c2.is_fraud,
      fn.depth + 1,
      CONCAT(fn.path, ' -> ', c2.claim_id) as path
    FROM fraud_network fn
    INNER JOIN {catalog}.{schema}.claims c1 ON fn.claim_id = c1.claim_id
    INNER JOIN {catalog}.{schema}.policyholders p1 ON c1.policyholder_id = p1.policyholder_id
    INNER JOIN {catalog}.{schema}.policyholders p2 ON (
      (p1.address = p2.address AND p1.address IS NOT NULL) OR
      (p1.phone = p2.phone AND p1.phone IS NOT NULL)
    )
    INNER JOIN {catalog}.{schema}.claims c2 ON c2.policyholder_id = p2.policyholder_id
    WHERE fn.depth < max_depth
      AND c2.claim_id != c1.claim_id
      AND fn.path NOT LIKE CONCAT('%', c2.claim_id, '%')
  )
  SELECT 
    claim_id,
    policyholder_id,
    claim_amount,
    is_fraud,
    depth,
    path
  FROM fraud_network
  ORDER BY depth, claim_id
  LIMIT 1000;
END
""")

# COMMAND ----------

# Stored Procedure 3: Get Claim Relationships (On-Demand)
# Computes relationships for a specific claim without pre-generation
spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.get_claim_relationships(
  target_claim_id STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
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
  SELECT * FROM related_claims;
END
""")

# COMMAND ----------

# Stored Procedure 4: Discover Fraud Network for Specific Policyholder (Optimized)
spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.discover_fraud_network_for_policyholder(
  target_policyholder_id STRING,
  max_depth INT DEFAULT 3
)
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Discover the fraud network starting from a specific policyholder ID (optimized for performance)'
AS
BEGIN
  WITH RECURSIVE fraud_network AS (
    -- Base case: Start from all claims of the given policyholder
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_amount,
      c.is_fraud,
      0 as depth,
      CAST(c.claim_id AS STRING) as path
    FROM {catalog}.{schema}.claims c
    WHERE c.policyholder_id = target_policyholder_id
    
    UNION ALL
    
    -- Recursive case: Find connected claims via shared policyholders (optimized)
    SELECT DISTINCT
      c2.claim_id,
      c2.policyholder_id,
      c2.claim_amount,
      c2.is_fraud,
      fn.depth + 1,
      CONCAT(fn.path, ' -> ', c2.claim_id) as path
    FROM fraud_network fn
    INNER JOIN {catalog}.{schema}.claims c1 ON fn.claim_id = c1.claim_id
    INNER JOIN {catalog}.{schema}.policyholders p1 ON c1.policyholder_id = p1.policyholder_id
    INNER JOIN {catalog}.{schema}.policyholders p2 ON (
      (p1.address = p2.address AND p1.address IS NOT NULL) OR
      (p1.phone = p2.phone AND p1.phone IS NOT NULL)
    )
    INNER JOIN {catalog}.{schema}.claims c2 ON c2.policyholder_id = p2.policyholder_id
    WHERE fn.depth < max_depth
      AND c2.claim_id != c1.claim_id
      AND fn.path NOT LIKE CONCAT('%', c2.claim_id, '%')
  )
  SELECT 
    claim_id,
    policyholder_id,
    claim_amount,
    is_fraud,
    depth,
    path
  FROM fraud_network
  ORDER BY depth, claim_id
  LIMIT 1000;
END
""")

# COMMAND ----------

print("✓ Stored procedures created successfully!")
print(f"  - {catalog}.{schema}.fraud_network_bfs")
print(f"  - {catalog}.{schema}.discover_fraud_network_for_claim")
print(f"  - {catalog}.{schema}.get_claim_relationships")
print(f"  - {catalog}.{schema}.discover_fraud_network_for_policyholder")
print("\nYou can now use these stored procedures as reusable tools!")
print("Note: All procedures use on-demand relationship computation and require specific claim/policyholder IDs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Discover Fraud Network for Specific Claims
# MAGIC 
# MAGIC Use stored procedures to discover fraud networks starting from specific claims or policyholders.
# MAGIC This approach is more practical for production use and avoids recursion limits.

# COMMAND ----------

# First, get a sample claim to explore
sample_claim = spark.sql(f"""
SELECT claim_id, policyholder_id, is_fraud, claim_amount
FROM {catalog}.{schema}.claims 
WHERE is_fraud = true 
LIMIT 1
""").collect()

if sample_claim:
    sample_claim_id = sample_claim[0]['claim_id']
    sample_policyholder_id = sample_claim[0]['policyholder_id']
    print(f"Sample fraudulent claim: {sample_claim_id}")
    print(f"Sample policyholder: {sample_policyholder_id}")
else:
    sample_claim_id = 'CLM00000001'
    sample_policyholder_id = 'PH000001'
    print(f"No fraudulent claims found, using examples")

# COMMAND ----------

# Discover fraud network starting from a specific claim
# Note: Using max_depth=3 for better performance (can be increased if needed)
result_df = spark.sql(f"""
CALL {catalog}.{schema}.discover_fraud_network_for_claim(
  start_claim_id => '{sample_claim_id}',
  max_depth => 3
)
""")
print(f"Found {result_df.count()} claims in the fraud network")
result_df.show(50, truncate=False)

# COMMAND ----------

# Discover fraud network starting from a specific policyholder
# Note: Using max_depth=3 for better performance (can be increased if needed)
result_df = spark.sql(f"""
CALL {catalog}.{schema}.discover_fraud_network_for_policyholder(
  target_policyholder_id => '{sample_policyholder_id}',
  max_depth => 3
)
""")
print(f"Found {result_df.count()} claims in the fraud network")
result_df.show(50, truncate=False)

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
# MAGIC ## Step 7: Summary Statistics (Non-Recursive Analysis)
# MAGIC 
# MAGIC These queries provide useful insights without using recursion, making them fast and reliable for large datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fraud Statistics by Claim Type
# MAGIC 
# MAGIC This shows which claim types have the highest fraud rates.

# COMMAND ----------

fraud_by_type = spark.sql(f"""
SELECT 
  claim_type,
  COUNT(*) as total_claims,
  SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
  ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate,
  ROUND(AVG(claim_amount), 2) as avg_claim_amount,
  ROUND(SUM(CASE WHEN is_fraud THEN claim_amount ELSE 0 END), 2) as total_fraud_amount
FROM {catalog}.{schema}.claims
GROUP BY claim_type
ORDER BY fraud_rate DESC
""")
fraud_by_type.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### High-Risk Policyholders
# MAGIC 
# MAGIC Find policyholders with multiple claims or high claim amounts (potential fraud indicators).

# COMMAND ----------

high_risk_policyholders = spark.sql(f"""
SELECT 
  p.policyholder_id,
  p.name,
  p.city,
  p.state,
  COUNT(c.claim_id) as claim_count,
  SUM(CASE WHEN c.is_fraud THEN 1 ELSE 0 END) as fraud_count,
  ROUND(SUM(c.claim_amount), 2) as total_claim_amount,
  ROUND(AVG(c.claim_amount), 2) as avg_claim_amount
FROM {catalog}.{schema}.policyholders p
INNER JOIN {catalog}.{schema}.claims c ON p.policyholder_id = c.policyholder_id
GROUP BY p.policyholder_id, p.name, p.city, p.state
HAVING claim_count >= 3 OR SUM(c.claim_amount) > 100000
ORDER BY fraud_count DESC, total_claim_amount DESC
LIMIT 20
""")
high_risk_policyholders.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shared Address Analysis
# MAGIC 
# MAGIC Find addresses with multiple policyholders (potential fraud ring indicator).

# COMMAND ----------

shared_addresses = spark.sql(f"""
SELECT 
  p.address,
  p.city,
  p.state,
  COUNT(DISTINCT p.policyholder_id) as policyholder_count,
  COUNT(DISTINCT c.claim_id) as total_claims,
  SUM(CASE WHEN c.is_fraud THEN 1 ELSE 0 END) as fraud_claims,
  ROUND(SUM(c.claim_amount), 2) as total_claim_amount
FROM {catalog}.{schema}.policyholders p
INNER JOIN {catalog}.{schema}.claims c ON p.policyholder_id = c.policyholder_id
GROUP BY p.address, p.city, p.state
HAVING policyholder_count > 1
ORDER BY fraud_claims DESC, policyholder_count DESC
LIMIT 20
""")
shared_addresses.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Usage Notes
# MAGIC 
# MAGIC ### Recommended Workflow for Fraud Investigation:
# MAGIC 
# MAGIC 1. **Identify Suspicious Claims**: Use non-recursive queries (like Step 7) to find high-risk claims or policyholders
# MAGIC 2. **Investigate Specific Claims**: Use `discover_fraud_network_for_claim()` to explore networks around suspicious claims
# MAGIC 3. **Investigate Policyholders**: Use `discover_fraud_network_for_policyholder()` to explore all claims in a policyholder's network
# MAGIC 4. **Get Detailed Relationships**: Use `get_claim_relationships()` to see what connects two claims
# MAGIC 5. **BFS Exploration**: Use `fraud_network_bfs()` for detailed path analysis from a specific claim
# MAGIC 
# MAGIC ### Performance Tips:
# MAGIC 
# MAGIC - Start with `max_depth=2` or `max_depth=3` for initial exploration
# MAGIC - Only increase depth if you need to go further into the network
# MAGIC - Use the non-recursive queries in Step 7 to identify candidates for recursive investigation
# MAGIC - The stored procedures are optimized for individual claim/policyholder investigation, not bulk processing

