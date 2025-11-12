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
# MAGIC ## Step 3: Create Stored Procedures for Fraud Detection
# MAGIC 
# MAGIC We create two essential stored procedures that demonstrate recursive CTEs for fraud detection:
# MAGIC 
# MAGIC 1. **`discover_fraud_network`**: Discovers the entire network of related claims starting from a suspicious claim
# MAGIC 2. **`get_claim_relationships`**: Gets direct relationships for a specific claim (shows what makes claims "connected")
# MAGIC 
# MAGIC ### The Story:
# MAGIC 
# MAGIC A fraud analyst receives a suspicious claim. They use:
# MAGIC - `get_claim_relationships()` to see what other claims are directly related and why
# MAGIC - `discover_fraud_network()` to explore the full network using recursive CTEs, revealing potential fraud rings
# MAGIC 
# MAGIC These procedures can be called interactively or used as tools in agentic systems for automated fraud detection.

# COMMAND ----------

# Stored Procedure 1: Discover Fraud Network (Main Recursive Demo)
# This is the core demonstration of recursive CTEs for fraud detection
spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.discover_fraud_network(
  start_claim_id STRING,
  max_depth INT DEFAULT 3
)
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Discovers fraud network using recursive CTEs - starting from a specific claim, finds all connected claims through shared policyholders'
AS
BEGIN
  WITH RECURSIVE fraud_network AS (
    -- Base case: Start from the suspicious claim
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_amount,
      c.is_fraud,
      c.claim_type,
      c.claim_date,
      0 as depth,
      CAST(c.claim_id AS STRING) as path
    FROM {catalog}.{schema}.claims c
    WHERE c.claim_id = start_claim_id
    
    UNION ALL
    
    -- Recursive case: Find connected claims through shared policyholders
    -- This is where the recursion happens - we keep expanding the network
    SELECT DISTINCT
      c2.claim_id,
      c2.policyholder_id,
      c2.claim_amount,
      c2.is_fraud,
      c2.claim_type,
      c2.claim_date,
      fn.depth + 1,
      CONCAT(fn.path, ' -> ', c2.claim_id) as path
    FROM fraud_network fn
    INNER JOIN {catalog}.{schema}.claims c1 ON fn.claim_id = c1.claim_id
    INNER JOIN {catalog}.{schema}.policyholders p1 ON c1.policyholder_id = p1.policyholder_id
    INNER JOIN {catalog}.{schema}.policyholders p2 ON (
      -- Connection logic: policyholders are related if they share address or phone
      (p1.address = p2.address AND p1.address IS NOT NULL) OR
      (p1.phone = p2.phone AND p1.phone IS NOT NULL)
    )
    INNER JOIN {catalog}.{schema}.claims c2 ON c2.policyholder_id = p2.policyholder_id
    WHERE fn.depth < max_depth
      AND c2.claim_id != c1.claim_id
      AND fn.path NOT LIKE CONCAT('%', c2.claim_id, '%')  -- Prevent cycles
  )
  SELECT 
    claim_id,
    policyholder_id,
    claim_amount,
    is_fraud,
    claim_type,
    claim_date,
    depth,
    path
  FROM fraud_network
  ORDER BY depth, claim_id
  LIMIT 1000;
END
""")

# COMMAND ----------

# Stored Procedure 2: Get Claim Relationships (Supporting Analysis)
# Shows the relationship logic - helps understand WHY claims are connected
spark.sql(f"""
CREATE OR REPLACE PROCEDURE {catalog}.{schema}.get_claim_relationships(
  target_claim_id STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Gets direct relationships for a claim - shows policyholder connections, temporal patterns, and service provider links'
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
  )
  SELECT DISTINCT
    c.claim_id as related_claim_id,
    c.claim_amount,
    c.is_fraud,
    c.claim_type,
    CASE 
      WHEN (p.address = tc.address AND p.address IS NOT NULL) THEN 'Same Address'
      WHEN (p.phone = tc.phone AND p.phone IS NOT NULL) THEN 'Same Phone'
      WHEN (c.claim_type = tc.claim_type AND 
            ABS(DATEDIFF(c.claim_date, tc.claim_date)) < 30 AND
            ABS(c.claim_amount - tc.claim_amount) < tc.claim_amount * 0.3) THEN 'Temporal Pattern'
      WHEN (c.adjuster_id = tc.adjuster_id AND 
            ABS(DATEDIFF(c.claim_date, tc.claim_date)) < 90) THEN 'Same Adjuster'
      ELSE 'Unknown'
    END as relationship_type
  FROM {catalog}.{schema}.claims c
  INNER JOIN {catalog}.{schema}.policyholders p ON c.policyholder_id = p.policyholder_id
  CROSS JOIN target_claim tc
  WHERE c.claim_id != tc.claim_id
    AND (
      (p.address = tc.address AND p.address IS NOT NULL) OR
      (p.phone = tc.phone AND p.phone IS NOT NULL) OR
      (c.claim_type = tc.claim_type AND 
       ABS(DATEDIFF(c.claim_date, tc.claim_date)) < 30 AND
       ABS(c.claim_amount - tc.claim_amount) < tc.claim_amount * 0.3) OR
      (c.adjuster_id = tc.adjuster_id AND 
       ABS(DATEDIFF(c.claim_date, tc.claim_date)) < 90)
    )
  ORDER BY relationship_type, related_claim_id
  LIMIT 100;
END
""")

# COMMAND ----------

print("✓ Stored procedures created successfully!")
print(f"\n📊 Available Procedures:")
print(f"  1. {catalog}.{schema}.discover_fraud_network(claim_id, max_depth)")
print(f"     → Discovers entire fraud network using recursive CTEs")
print(f"\n  2. {catalog}.{schema}.get_claim_relationships(claim_id)")
print(f"     → Shows direct relationships and why claims are connected")
print(f"\n💡 These procedures use on-demand relationship computation (production approach)")
print(f"   No pre-generated relationship tables needed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Using the Stored Procedures - The Fraud Investigation Story
# MAGIC 
# MAGIC Let's walk through a typical fraud investigation workflow using our recursive stored procedures.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4a: Identify a Suspicious Claim
# MAGIC 
# MAGIC First, we identify a suspicious claim to investigate. In production, this might come from:
# MAGIC - Automated fraud detection models
# MAGIC - Manual reports from adjusters
# MAGIC - Pattern analysis (high value, multiple claims, etc.)

# COMMAND ----------

# Find a high-value fraudulent claim to investigate
suspicious_claims = spark.sql(f"""
SELECT 
  claim_id,
  policyholder_id,
  claim_type,
  claim_amount,
  claim_date,
  is_fraud
FROM {catalog}.{schema}.claims 
WHERE is_fraud = true
  AND claim_amount > 10000
ORDER BY claim_amount DESC
LIMIT 5
""")

print("🔍 Top suspicious claims:")
suspicious_claims.show(truncate=False)

# Get the first one for investigation
sample_claim = suspicious_claims.collect()[0]
target_claim_id = sample_claim['claim_id']
print(f"\n📌 Investigating claim: {target_claim_id}")
print(f"   Amount: ${sample_claim['claim_amount']:,.2f}")
print(f"   Type: {sample_claim['claim_type']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4b: Understand Direct Relationships
# MAGIC 
# MAGIC Before exploring the full network, let's see what makes this claim suspicious by examining its direct relationships.

# COMMAND ----------

# Get direct relationships for the suspicious claim
print(f"🔗 Finding direct relationships for claim {target_claim_id}...\n")

relationships_df = spark.sql(f"""
CALL {catalog}.{schema}.get_claim_relationships('{target_claim_id}')
""")

relationship_count = relationships_df.count()
print(f"Found {relationship_count} related claims")

if relationship_count > 0:
    relationships_df.show(20, truncate=False)
    
    # Summary by relationship type
    print("\n📊 Relationship Type Summary:")
    relationships_df.groupBy("relationship_type").count().orderBy("count", ascending=False).show()
else:
    print("No direct relationships found for this claim")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4c: Discover the Full Fraud Network
# MAGIC 
# MAGIC Now we use the recursive CTE stored procedure to discover the entire fraud network.
# MAGIC This is where the **recursion magic** happens - we traverse through connected claims to reveal potential fraud rings!

# COMMAND ----------

# Discover the full fraud network using recursive CTEs
print(f"🕸️  Discovering fraud network for claim {target_claim_id}...")
print(f"   Using recursive CTEs with max_depth=3\n")

network_df = spark.sql(f"""
CALL {catalog}.{schema}.discover_fraud_network(
  start_claim_id => '{target_claim_id}',
  max_depth => 3
)
""")

network_count = network_df.count()
print(f"✓ Found {network_count} claims in the fraud network")

if network_count > 0:
    # Show the network
    network_df.show(50, truncate=False)
    
    # Network statistics
    print("\n📈 Network Statistics:")
    network_stats = network_df.groupBy("depth").agg(
        count("*").alias("claim_count"),
        sum("claim_amount").alias("total_amount"),
        sum(when(col("is_fraud"), 1).otherwise(0)).alias("fraud_count")
    ).orderBy("depth")
    network_stats.show()
    
    # Total network impact
    total_amount = network_df.agg(sum("claim_amount")).collect()[0][0]
    total_fraud = network_df.filter(col("is_fraud")).count()
    print(f"\n💰 Total Network Value: ${total_amount:,.2f}")
    print(f"🚨 Fraudulent Claims in Network: {total_fraud}/{network_count} ({total_fraud/network_count*100:.1f}%)")
else:
    print("This claim appears to be isolated (no network connections found)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Summary Statistics (Non-Recursive Analysis)
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
# MAGIC ## Production Usage Guide
# MAGIC 
# MAGIC ### Recommended Workflow for Fraud Investigation:
# MAGIC 
# MAGIC 1. **Identify Suspicious Claims**: Use non-recursive queries (Step 5) or ML models to find high-risk claims
# MAGIC 2. **Understand Relationships**: Call `get_claim_relationships(claim_id)` to see direct connections
# MAGIC 3. **Discover Network**: Call `discover_fraud_network(claim_id, depth)` to reveal the full fraud ring using recursive CTEs
# MAGIC 4. **Take Action**: Prioritize investigation of connected high-value or fraudulent claims in the network
# MAGIC 
# MAGIC ### Performance Tips:
# MAGIC 
# MAGIC - **Start with `max_depth=2` or `3`**: Recursion grows exponentially, so start shallow
# MAGIC - **Investigate specific claims**: These procedures are designed for targeted investigation, not bulk processing
# MAGIC - **Use Step 5 for bulk analysis**: Non-recursive queries are faster for finding candidates
# MAGIC - **Adjust based on results**: If you find a large network, reduce depth; if isolated, try increasing
# MAGIC 
# MAGIC ### Integration with Agentic Systems:
# MAGIC 
# MAGIC These stored procedures can be called by AI agents:
# MAGIC ```python
# MAGIC # Agent receives suspicious claim ID
# MAGIC result = spark.sql(f"CALL discover_fraud_network('{claim_id}', 3)")
# MAGIC # Agent analyzes network and takes action
# MAGIC ```

