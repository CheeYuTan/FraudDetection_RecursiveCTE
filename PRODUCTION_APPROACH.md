# Production Approach: Deriving Relationships from Real Data

This document explains how to adapt this demo for production use with real insurance claim data.

## Key Difference: Demo vs Production

**Demo (Synthetic Data):**
- Fraud rings are pre-labeled
- Relationships are pre-generated
- `is_fraud` flag is known

**Production (Real Data):**
- Fraud rings must be **discovered** through analysis
- Relationships must be **derived** from actual data patterns
- Fraud status may be unknown or partially known

## Step 1: Derive Relationships from Raw Data

In production, you would create the `claim_relationships` table by analyzing actual claim data for patterns:

### Example: Create Relationships from Shared Attributes

```sql
-- Create relationships based on shared policyholder attributes
CREATE OR REPLACE TABLE claim_relationships AS
WITH shared_attributes AS (
  -- Same address
  SELECT DISTINCT
    c1.claim_id as claim_id_1,
    c2.claim_id as claim_id_2,
    'shared_address' as relationship_type,
    0.8 as strength
  FROM claims c1
  INNER JOIN claims c2 ON c1.policyholder_id = c2.policyholder_id
  WHERE c1.claim_id < c2.claim_id
    AND c1.address = c2.address
    AND c1.address IS NOT NULL
  
  UNION ALL
  
  -- Same phone number
  SELECT DISTINCT
    c1.claim_id as claim_id_1,
    c2.claim_id as claim_id_2,
    'shared_phone' as relationship_type,
    0.7 as strength
  FROM claims c1
  INNER JOIN claims c2 ON c1.policyholder_id = c2.policyholder_id
  WHERE c1.claim_id < c2.claim_id
    AND c1.phone = c2.phone
    AND c1.phone IS NOT NULL
  
  UNION ALL
  
  -- Same adjuster with similar patterns
  SELECT DISTINCT
    c1.claim_id as claim_id_1,
    c2.claim_id as claim_id_2,
    'same_adjuster_similar_pattern' as relationship_type,
    0.5 as strength
  FROM claims c1
  INNER JOIN claims c2 ON c1.adjuster_id = c2.adjuster_id
  WHERE c1.claim_id < c2.claim_id
    AND c1.claim_type = c2.claim_type
    AND ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 30
    AND ABS(c1.claim_amount - c2.claim_amount) < c1.claim_amount * 0.2
  
  UNION ALL
  
  -- Geographic proximity
  SELECT DISTINCT
    c1.claim_id as claim_id_1,
    c2.claim_id as claim_id_2,
    'geographic_proximity' as relationship_type,
    0.4 as strength
  FROM claims c1
  INNER JOIN policyholders p1 ON c1.policyholder_id = p1.policyholder_id
  INNER JOIN claims c2 ON c1.claim_type = c2.claim_type
  INNER JOIN policyholders p2 ON c2.policyholder_id = p2.policyholder_id
  WHERE c1.claim_id < c2.claim_id
    AND p1.city = p2.city
    AND p1.state = p2.state
    AND ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 90
)
SELECT * FROM shared_attributes;
```

### Alternative: Use Graph Analysis

For more sophisticated relationship detection, you could use graph algorithms:

```python
from pyspark.sql import GraphFrame
from pyspark.sql.functions import col

# Create vertices (claims)
vertices = claims_df.select(
    col("claim_id").alias("id"),
    col("policyholder_id"),
    col("claim_type"),
    col("claim_amount"),
    col("claim_date")
)

# Create edges (relationships based on shared attributes)
edges = spark.sql("""
  SELECT 
    c1.claim_id as src,
    c2.claim_id as dst,
    'shared_address' as relationship
  FROM claims c1
  JOIN claims c2 ON c1.policyholder_id = c2.policyholder_id
  WHERE c1.address = c2.address
    AND c1.claim_id < c2.claim_id
""")

# Build graph
graph = GraphFrame(vertices, edges)

# Find connected components (potential fraud rings)
connected_components = graph.connectedComponents()
```

## Step 2: Discover Fraud Rings Using Recursive Queries

Once you have relationships, use recursive CTEs to discover fraud networks:

### Start with Known Fraud Cases

```sql
-- Start with confirmed fraud cases (from investigations, ML models, etc.)
WITH RECURSIVE fraud_network AS (
  -- Base case: Known fraudulent claims
  SELECT 
    claim_id,
    policyholder_id,
    0 as depth,
    CAST(claim_id AS STRING) as path
  FROM claims
  WHERE is_fraud = true  -- Or use your fraud detection model score
    OR fraud_score > 0.8  -- High-risk claims from ML model
  
  UNION ALL
  
  -- Recursive case: Find connected claims
  SELECT 
    c.claim_id,
    c.policyholder_id,
    fn.depth + 1,
    CONCAT(fn.path, ' -> ', c.claim_id)
  FROM fraud_network fn
  INNER JOIN claim_relationships cr 
    ON (fn.claim_id = cr.claim_id_1 OR fn.claim_id = cr.claim_id_2)
  INNER JOIN claims c
    ON (c.claim_id = CASE 
        WHEN fn.claim_id = cr.claim_id_1 THEN cr.claim_id_2 
        ELSE cr.claim_id_1 
      END)
  WHERE fn.depth < 5
    AND c.claim_id != fn.claim_id
    AND fn.path NOT LIKE CONCAT('%', c.claim_id, '%')
)
-- Identify potential fraud rings
SELECT 
  COUNT(DISTINCT claim_id) as network_size,
  COUNT(DISTINCT policyholder_id) as unique_policyholders,
  COLLECT_SET(claim_id) as network_claims
FROM fraud_network
GROUP BY root_claim_id
HAVING network_size > 3  -- Threshold for fraud ring
ORDER BY network_size DESC;
```

## Step 3: Assign Fraud Ring IDs

After discovering networks, assign ring IDs:

```sql
-- Assign fraud ring IDs to discovered networks
WITH discovered_rings AS (
  -- Use the recursive query above to find networks
  SELECT 
    claim_id,
    ROW_NUMBER() OVER (ORDER BY network_size DESC) as ring_id
  FROM fraud_network
  WHERE network_size > 3
)
UPDATE claims c
SET fraud_ring_id = CONCAT('RING', dr.ring_id)
FROM discovered_rings dr
WHERE c.claim_id = dr.claim_id;
```

## Step 4: Production Data Requirements

For production, you would need:

### Required Tables
- **claims**: Raw claim data with attributes
- **policyholders**: Policyholder information
- **claim_relationships**: Derived from data patterns (not pre-labeled)

### Optional but Helpful
- **adjusters**: Adjuster assignments
- **service_providers**: Repair shops, medical providers, etc.
- **investigations**: Known fraud cases for base case
- **fraud_scores**: ML model predictions

## Step 5: Incremental Updates

In production, relationships and fraud rings should be updated incrementally:

```sql
-- Update relationships for new claims
INSERT INTO claim_relationships
SELECT ... -- New relationships for claims added since last run

-- Re-run recursive analysis to discover new fraud rings
-- Update fraud_ring_id assignments
```

## Best Practices

1. **Start Small**: Begin with high-confidence relationships (shared addresses, phone numbers)
2. **Iterate**: Gradually add more relationship types as you validate patterns
3. **Validate**: Manually review discovered networks before flagging as fraud rings
4. **Monitor**: Track false positive rates and adjust relationship strength thresholds
5. **Combine Sources**: Use multiple relationship types for more robust detection

## Example Production Workflow

1. **Daily/Weekly**: Derive relationships from new claims
2. **Daily/Weekly**: Run recursive queries to discover new networks
3. **Review**: Fraud analysts review discovered networks
4. **Assign**: Assign fraud_ring_id to confirmed fraud rings
5. **Alert**: Generate alerts for high-risk networks

This approach allows you to discover fraud rings organically from your actual data, rather than relying on pre-labeled synthetic data.

