-- Recursive Fraud Detection SQL Queries
-- These queries can be run in Databricks SQL or Spark SQL

-- ============================================
-- Query 1: Find Fraud Networks Using Recursive CTE
-- ============================================
WITH RECURSIVE fraud_network AS (
  -- Base case: Start with known fraudulent claims
  SELECT 
    c.claim_id,
    c.policyholder_id,
    c.claim_amount,
    c.is_fraud,
    c.fraud_ring_id,
    0 as depth,
    CAST(c.claim_id AS STRING) as path,
    c.claim_id as root_claim_id
  FROM fraud_detection_demo.claims c
  WHERE c.is_fraud = true
  
  UNION ALL
  
  -- Recursive case: Find connected claims
  SELECT 
    c.claim_id,
    c.policyholder_id,
    c.claim_amount,
    c.is_fraud,
    c.fraud_ring_id,
    fn.depth + 1,
    CONCAT(fn.path, ' -> ', c.claim_id) as path,
    fn.root_claim_id
  FROM fraud_network fn
  INNER JOIN fraud_detection_demo.claim_relationships cr 
    ON (fn.claim_id = cr.claim_id_1 OR fn.claim_id = cr.claim_id_2)
  INNER JOIN fraud_detection_demo.claims c
    ON (c.claim_id = CASE 
        WHEN fn.claim_id = cr.claim_id_1 THEN cr.claim_id_2 
        ELSE cr.claim_id_1 
      END)
  WHERE fn.depth < 5  -- Limit recursion depth
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
HAVING network_size > 1
ORDER BY network_size DESC, total_network_amount DESC;

-- ============================================
-- Query 2: Policyholder Network Analysis
-- ============================================
WITH policyholder_connections AS (
  -- Direct connections: policyholders with shared claim relationships
  SELECT DISTINCT
    c1.policyholder_id as ph1,
    c2.policyholder_id as ph2,
    1 as connection_strength
  FROM fraud_detection_demo.claims c1
  INNER JOIN fraud_detection_demo.claim_relationships cr 
    ON c1.claim_id = cr.claim_id_1
  INNER JOIN fraud_detection_demo.claims c2
    ON c2.claim_id = cr.claim_id_2
  WHERE c1.policyholder_id != c2.policyholder_id
),
RECURSIVE policyholder_network AS (
  -- Base case: Start with known fraud ring members
  SELECT 
    ph.policyholder_id,
    ph.policyholder_id as root_policyholder,
    0 as depth,
    CAST(ph.policyholder_id AS STRING) as path
  FROM fraud_detection_demo.policyholders ph
  WHERE ph.is_fraud_ring_member = true
  
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
ORDER BY network_size DESC;

-- ============================================
-- Query 3: Suspicious Claim Chains
-- ============================================
WITH RECURSIVE suspicious_chain AS (
  -- Base case: High-value claims or claims with multiple relationships
  SELECT 
    c.claim_id,
    c.policyholder_id,
    c.claim_amount,
    c.claim_date,
    0 as chain_length,
    CAST(c.claim_id AS STRING) as chain_path,
    c.claim_id as chain_start
  FROM fraud_detection_demo.claims c
  WHERE c.claim_amount > 50000
    OR (
      SELECT COUNT(*) 
      FROM fraud_detection_demo.claim_relationships cr 
      WHERE cr.claim_id_1 = c.claim_id OR cr.claim_id_2 = c.claim_id
    ) > 3
  
  UNION ALL
  
  -- Recursive case: Find connected suspicious claims
  SELECT 
    c.claim_id,
    c.policyholder_id,
    c.claim_amount,
    c.claim_date,
    sc.chain_length + 1,
    CONCAT(sc.chain_path, ' -> ', c.claim_id),
    sc.chain_start
  FROM suspicious_chain sc
  INNER JOIN fraud_detection_demo.claim_relationships cr
    ON (sc.claim_id = cr.claim_id_1 OR sc.claim_id = cr.claim_id_2)
  INNER JOIN fraud_detection_demo.claims c
    ON (c.claim_id = CASE 
        WHEN sc.claim_id = cr.claim_id_1 THEN cr.claim_id_2 
        ELSE cr.claim_id_1 
      END)
  WHERE sc.chain_length < 4
    AND c.claim_id != sc.claim_id
    AND sc.chain_path NOT LIKE CONCAT('%', c.claim_id, '%')
    AND (
      c.claim_amount > 30000
      OR ABS(DATEDIFF(c.claim_date, sc.claim_date)) < 90
    )
)
SELECT 
  chain_start,
  COUNT(DISTINCT claim_id) as chain_size,
  SUM(claim_amount) as total_chain_amount,
  MAX(chain_length) as max_chain_length,
  COLLECT_SET(claim_id) as chain_claims
FROM suspicious_chain
GROUP BY chain_start
HAVING chain_size >= 3
ORDER BY total_chain_amount DESC, chain_size DESC;

