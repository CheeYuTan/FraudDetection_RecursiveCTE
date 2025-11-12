-- Fraud Detection Stored Procedures
-- Based on patterns from: https://medium.com/dbsql-sme-engineering/graph-analytics-with-dbsql-stored-procedures-and-agents-5e53bad68032
-- 
-- These stored procedures package recursive fraud detection queries into reusable components
-- Requirements: Databricks Runtime 17.0 or later

-- ============================================
-- Stored Procedure 1: Fraud Network Discovery (BFS)
-- ============================================
-- Finds all claims connected to a starting claim using breadth-first search
CREATE OR REPLACE PROCEDURE fraud_network_bfs(
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
    -- Base case: Start with the specified claim
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_amount,
      c.is_fraud,
      0 as depth,
      CAST(c.claim_id AS STRING) as path,
      c.claim_id as root_claim_id
    FROM claims c
    WHERE c.claim_id = start_claim_id
    
    UNION ALL
    
    -- Recursive case: Find connected claims
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_amount,
      c.is_fraud,
      fn.depth + 1,
      CONCAT(fn.path, ' -> ', c.claim_id) as path,
      fn.root_claim_id
    FROM fraud_network fn
    INNER JOIN claim_relationships cr 
      ON (fn.claim_id = cr.claim_id_1 OR fn.claim_id = cr.claim_id_2)
    INNER JOIN claims c
      ON (c.claim_id = CASE 
          WHEN fn.claim_id = cr.claim_id_1 THEN cr.claim_id_2 
          ELSE cr.claim_id_1 
        END)
    WHERE fn.depth < max_depth
      AND c.claim_id != fn.claim_id
      AND fn.path NOT LIKE CONCAT('%', c.claim_id, '%')  -- Prevent cycles
  )
  SELECT * FROM fraud_network
$$;

-- ============================================
-- Stored Procedure 2: Find Fraud Networks from Known Fraud
-- ============================================
-- Discovers all fraud networks starting from known fraudulent claims
CREATE OR REPLACE PROCEDURE discover_fraud_networks(
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
    -- Base case: Start with known fraudulent claims
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_amount,
      c.is_fraud,
      0 as depth,
      CAST(c.claim_id AS STRING) as path,
      c.claim_id as root_claim_id
    FROM claims c
    WHERE c.is_fraud = true
    
    UNION ALL
    
    -- Recursive case: Find connected claims
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_amount,
      c.is_fraud,
      fn.depth + 1,
      CONCAT(fn.path, ' -> ', c.claim_id) as path,
      fn.root_claim_id
    FROM fraud_network fn
    INNER JOIN claim_relationships cr 
      ON (fn.claim_id = cr.claim_id_1 OR fn.claim_id = cr.claim_id_2)
    INNER JOIN claims c
      ON (c.claim_id = CASE 
          WHEN fn.claim_id = cr.claim_id_1 THEN cr.claim_id_2 
          ELSE cr.claim_id_1 
        END)
    WHERE fn.depth < max_depth
      AND c.claim_id != fn.claim_id
      AND fn.path NOT LIKE CONCAT('%', c.claim_id, '%')  -- Prevent cycles
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
$$;

-- ============================================
-- Stored Procedure 3: Compute Relationships On-Demand
-- ============================================
-- Computes relationships for a specific claim on-demand (avoids O(n²) upfront cost)
CREATE OR REPLACE PROCEDURE get_claim_relationships(
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
    FROM claims c
    INNER JOIN policyholders p ON c.policyholder_id = p.policyholder_id
    WHERE c.claim_id = target_claim_id
  ),
  related_claims AS (
    -- 1. Policyholder connections: Same address or phone
    SELECT DISTINCT
      c.claim_id as related_claim_id,
      'policyholder_connection' as relationship_type,
      0.8 as strength
    FROM claims c
    INNER JOIN policyholders p ON c.policyholder_id = p.policyholder_id
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
    FROM claims c
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
    FROM claims c
    CROSS JOIN target_claim tc
    WHERE c.claim_id != tc.claim_id
      AND c.adjuster_id = tc.adjuster_id
      AND ABS(DATEDIFF(c.claim_date, tc.claim_date)) < 90
  )
  SELECT * FROM related_claims
$$;

-- ============================================
-- Stored Procedure 4: Recursive Fraud Network with On-Demand Relationships
-- ============================================
-- Uses on-demand relationship computation (no pre-generated relationships table needed)
CREATE OR REPLACE PROCEDURE discover_fraud_networks_ondemand(
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
    -- Base case: Start with known fraudulent claims
    SELECT 
      c.claim_id,
      c.policyholder_id,
      c.claim_amount,
      c.is_fraud,
      0 as depth,
      CAST(c.claim_id AS STRING) as path,
      c.claim_id as root_claim_id
    FROM claims c
    WHERE c.is_fraud = true
    
    UNION ALL
    
    -- Recursive case: Find connected claims using on-demand relationship computation
    SELECT 
      c2.claim_id,
      c2.policyholder_id,
      c2.claim_amount,
      c2.is_fraud,
      fn.depth + 1,
      CONCAT(fn.path, ' -> ', c2.claim_id) as path,
      fn.root_claim_id
    FROM fraud_network fn
    INNER JOIN claims c1 ON fn.claim_id = c1.claim_id
    INNER JOIN policyholders p1 ON c1.policyholder_id = p1.policyholder_id
    INNER JOIN claims c2 ON c2.claim_id != c1.claim_id
    INNER JOIN policyholders p2 ON c2.policyholder_id = p2.policyholder_id
    WHERE fn.depth < max_depth
      AND fn.path NOT LIKE CONCAT('%', c2.claim_id, '%')  -- Prevent cycles
      AND (
        -- 1. Policyholder connections: Same address or phone
        (p1.address = p2.address AND p1.address IS NOT NULL) OR
        (p1.phone = p2.phone AND p1.phone IS NOT NULL) OR
        -- 2. Temporal patterns: Same claim type, within 30 days, similar amounts
        (c1.claim_type = c2.claim_type AND 
         ABS(DATEDIFF(c1.claim_date, c2.claim_date)) < 30 AND
         ABS(c1.claim_amount - c2.claim_amount) < c1.claim_amount * 0.3) OR
        -- 3. Service provider connections: Same adjuster, within 90 days
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
$$;

