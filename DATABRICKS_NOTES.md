# Databricks Implementation Notes

## Recursive CTE Support

Recursive Common Table Expressions (CTEs) are supported in:
- **Databricks SQL**: Runtime 9.1+ (Photon enabled)
- **Spark SQL**: Version 3.0+ with recursive CTE support
- **Databricks Runtime**: 9.1 LTS or higher

### Checking Your Version

Run this in a Databricks notebook:
```python
spark.sql("SELECT version()").show()
```

Or in SQL:
```sql
SELECT version();
```

## Alternative Approaches

If recursive CTEs are not available, you can use:

### 1. Iterative Approach with Python
```python
# Instead of recursive CTE, use iterative joins
max_depth = 5
current_level = spark.sql("SELECT * FROM claims WHERE is_fraud = true")
all_levels = current_level

for depth in range(max_depth):
    next_level = spark.sql(f"""
        SELECT DISTINCT c.*
        FROM ({current_level.createOrReplaceTempView('current')}) current
        JOIN claim_relationships cr ON current.claim_id = cr.claim_id_1
        JOIN claims c ON c.claim_id = cr.claim_id_2
        WHERE c.claim_id NOT IN (SELECT claim_id FROM all_levels)
    """)
    all_levels = all_levels.union(next_level)
    current_level = next_level
```

### 2. GraphX (Spark Graph Processing)
```python
from pyspark.sql import GraphFrame

# Create vertices and edges
vertices = claims_df.select("claim_id", "policyholder_id", "is_fraud")
edges = relationships_df.select("claim_id_1", "claim_id_2", "relationship_type")

# Create graph
graph = GraphFrame(vertices, edges)

# Find connected components
connected_components = graph.connectedComponents()
```

### 3. Limited Depth Joins
```sql
-- Instead of recursion, use multiple joins
WITH level1 AS (
  SELECT * FROM claims WHERE is_fraud = true
),
level2 AS (
  SELECT c.* FROM level1 l1
  JOIN claim_relationships cr ON l1.claim_id = cr.claim_id_1
  JOIN claims c ON c.claim_id = cr.claim_id_2
),
level3 AS (
  SELECT c.* FROM level2 l2
  JOIN claim_relationships cr ON l2.claim_id = cr.claim_id_1
  JOIN claims c ON c.claim_id = cr.claim_id_2
)
SELECT * FROM level1
UNION SELECT * FROM level2
UNION SELECT * FROM level3;
```

## File Path Adjustments

Depending on your Databricks setup, you may need to adjust file paths:

### DBFS Paths
```python
# Standard DBFS path
path = "/dbfs/FileStore/fraud_demo/claims.csv"

# Alternative DBFS path
path = "dbfs:/FileStore/fraud_demo/claims.csv"
```

### Workspace Files
```python
# If files are in workspace
path = "/Workspace/Users/your_email@domain.com/fraud_demo/data/claims.csv"
```

### External Storage
```python
# S3, ADLS, etc.
path = "s3://your-bucket/fraud_demo/claims.csv"
path = "abfss://container@account.dfs.core.windows.net/fraud_demo/claims.csv"
```

## Performance Optimization

### 1. Caching Intermediate Results
```python
# Cache frequently used DataFrames
claims_df.cache()
relationships_df.cache()
```

### 2. Partitioning
```python
# Partition large tables
claims_df.write \
  .partitionBy("claim_type", "claim_date") \
  .saveAsTable("fraud_detection_demo.claims")
```

### 3. Z-Ordering
```python
# Optimize for queries
spark.sql("""
  OPTIMIZE fraud_detection_demo.claims
  ZORDER BY (claim_date, claim_amount)
""")
```

## Cluster Configuration

Recommended cluster settings:
- **Runtime**: Databricks Runtime 11.3 LTS or higher
- **Node Type**: Standard (for demo) or Memory-optimized (for production)
- **Workers**: 2-4 workers for demo, scale up for production
- **Photon**: Enable for better SQL performance

## Troubleshooting

### Issue: "Recursive CTE syntax error"
**Solution**: Check if your Databricks version supports recursive CTEs. Use alternative approaches above.

### Issue: "Out of memory during recursion"
**Solution**: 
- Reduce recursion depth
- Add more restrictive WHERE clauses
- Increase cluster memory
- Use checkpointing for large datasets

### Issue: "Slow query performance"
**Solution**:
- Enable Photon acceleration
- Cache intermediate results
- Optimize table layout (partitioning, Z-ordering)
- Use Delta Lake optimizations

## Migration from Other Platforms

### From PostgreSQL
- Recursive CTE syntax is similar
- May need to adjust `CONCAT` vs `||` for string concatenation
- Use `COLLECT_SET` instead of `ARRAY_AGG`

### From SQL Server
- Similar recursive CTE syntax
- Use `COLLECT_SET` instead of `STRING_AGG`
- Adjust date functions (`DATEDIFF` syntax may differ)

### From Oracle
- Similar recursive CTE with `CONNECT BY`
- Use `COLLECT_SET` instead of `LISTAGG`
- Adjust date arithmetic

## Best Practices

1. **Limit Recursion Depth**: Always set a maximum depth to prevent infinite loops
2. **Cycle Detection**: Include cycle detection in recursive queries
3. **Index Relationships**: Create indexes on join columns
4. **Monitor Performance**: Use query profiling to identify bottlenecks
5. **Test Incrementally**: Start with small datasets, then scale up

