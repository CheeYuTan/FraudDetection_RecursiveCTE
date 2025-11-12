# Insurance Fraud Detection Demo - Databricks Recursive Capabilities

This demo showcases how to use Databricks recursive SQL capabilities to detect insurance claim fraud through network analysis and pattern recognition.

## Overview

The demo includes:
- **Synthetic Dataset**: Realistic insurance claim data with fraud patterns and networks
- **Recursive SQL Queries**: Advanced queries to detect fraud rings and connected claims
- **Network Analysis**: Identify policyholder networks and suspicious claim chains
- **Fraud Risk Scoring**: Calculate risk scores based on network connections

## Project Structure

```
.
├── README.md                          # This file
├── notebooks/                         # Databricks notebooks
│   ├── 01_Dataset_Generation.py     # ⭐ Generate dataset directly in Databricks
│   ├── 02_Recursive_Fraud_Detection.py  # Recursive fraud detection queries
│   └── 03_Fraud_Analysis_Visualization.py  # Analysis and visualization
└── sql_scripts/                       # Standalone SQL scripts
    └── recursive_fraud_queries.sql    # Recursive SQL queries
```

**Clean and minimal!** All data generation happens directly in Databricks - no CSV files or local scripts needed.

## Prerequisites

- Databricks workspace (or Databricks Community Edition)
- **Databricks Runtime 17.0 or later** (required for recursive CTE support and optimized performance)

## Setup Instructions

### Step 1: Import Databricks Notebooks

1. In your Databricks workspace, go to **Workspace**
2. Create a new folder (e.g., `Fraud_Detection_Demo`)
3. Import all 3 notebooks from the `notebooks/` directory:
   - `01_Dataset_Generation.py`
   - `02_Recursive_Fraud_Detection.py`
   - `03_Fraud_Analysis_Visualization.py`

### Step 2: Generate Dataset

1. Open the `01_Dataset_Generation.py` notebook
2. Configure the widgets at the top of the notebook:
   - **Catalog**: Your catalog name (default: `main`)
   - **Schema**: Your schema/database name (default: `fraud_detection_demo`)
   - **Volume Scale**: Choose from small, medium, large, xlarge, or custom
   - **Number of Policyholders**: Used if custom scale (default: 1000)
   - **Number of Claims**: Used if custom scale (default: 5000)
   - **Fraud Rate**: Default 0.15 (15%)
   - **Number of Adjusters**: Default 50
   - **Batch Size**: For large datasets (default: 1,000,000)
   - **Overwrite Mode**: true/false
3. Run the notebook - it will generate the data and write directly to Delta tables

### Step 3: Run Fraud Detection Analysis

1. **02_Recursive_Fraud_Detection.py**: Runs recursive queries to detect fraud networks
   - Configure widgets: Catalog and Schema (must match Step 2)
2. **03_Fraud_Analysis_Visualization.py**: Provides analysis and visualizations
   - Configure widgets: Catalog and Schema (must match Step 2)

**Important:** All notebooks use widgets for catalog and schema configuration. Make sure to set the same catalog and schema values across all notebooks!

## Key Features

### Recursive Fraud Network Detection

The demo uses recursive Common Table Expressions (CTEs) to:
- Find all claims connected to known fraudulent claims
- Build fraud networks by traversing claim relationships
- Identify policyholder networks within fraud rings
- Detect suspicious claim chains

### Example Recursive Query

```sql
WITH RECURSIVE fraud_network AS (
  -- Base case: Start with known fraudulent claims
  SELECT claim_id, policyholder_id, 0 as depth
  FROM claims WHERE is_fraud = true
  
  UNION ALL
  
  -- Recursive case: Find connected claims
  SELECT c.claim_id, c.policyholder_id, fn.depth + 1
  FROM fraud_network fn
  JOIN claim_relationships cr ON fn.claim_id = cr.claim_id_1
  JOIN claims c ON c.claim_id = cr.claim_id_2
  WHERE fn.depth < 5
)
SELECT * FROM fraud_network;
```

### Fraud Risk Scoring

The system calculates fraud risk scores based on:
- Known fraud status (50 points)
- Membership in fraud network (30 points)
- High number of connections (20 points)
- High claim amount (15 points)
- Fraud ring membership (25 points)

Maximum score: 100 points

## Dataset Details

### Policyholders
- 1,000 unique policyholders
- Geographic distribution across 6 states
- Fraud ring membership indicators

### Claims
- 5,000 insurance claims
- ~15% fraud rate (750 fraudulent claims)
- Multiple claim types: Auto, Home, Health, Property, Liability
- Claim amounts ranging from $100 to $500,000

### Relationships
- Connections between related claims
- Fraud ring connections (strong relationships)
- Similar pattern connections (weaker relationships)

### Fraud Rings
- Multiple fraud rings with 5-15 members each
- Connected through claim relationships
- Higher fraud rates within rings

## Analysis Capabilities

The demo provides:

1. **Network Analysis**: Identify connected fraud networks
2. **Pattern Detection**: Find suspicious claim patterns
3. **Geographic Analysis**: Fraud rates by location
4. **Temporal Analysis**: Fraud trends over time
5. **Risk Scoring**: Automated fraud risk assessment

## Customization

### Adjusting Dataset Parameters

Use the widgets in `01_Dataset_Generation.py` to adjust:

**Volume Scale (Dropdown):**
- **small**: 1K policyholders, 5K claims (default - for testing)
- **medium**: 10K policyholders, 50K claims
- **large**: 100K policyholders, 1M claims
- **xlarge**: 1M policyholders, 10M claims
- **custom**: Use custom values from widgets below

**For 100M+ records:**
1. Select **custom** volume scale
2. Set **Number of Claims** to `100000000` (or your target)
3. Set **Number of Policyholders** proportionally (e.g., `10000000`)
4. Adjust **Batch Size** (recommended: 1M-10M for very large datasets)
5. Use a larger cluster with more workers

**Other Parameters:**
- Fraud rate (0.0-1.0)
- Number of adjusters
- Catalog and schema names
- Overwrite mode

Simply change the widget values and re-run the notebook!

### Modifying Recursion Depth

In the recursive queries, adjust the depth limit:
```sql
WHERE fn.depth < 5  -- Change to desired depth
```

### Adding More Data

Increase the number of records:
```python
policyholders_df = generate_policyholders(n=5000)  # More policyholders
claims_df, fraud_rings = generate_claims(policyholders_df, n_claims=20000)  # More claims
```

## Troubleshooting

### Issue: Recursive CTE not supported

**Solution**: Ensure you're using **Databricks Runtime 17.0 or later**. Recursive CTEs are only supported in Runtime 17.0 and later.

### Issue: Files not found in DBFS

**Solution**: Verify file paths. Use:
```python
dbutils.fs.ls("/FileStore/fraud_demo/")
```

### Issue: Memory errors with large datasets

**Solution**: 
- Reduce recursion depth
- Limit the number of base cases
- Use more restrictive WHERE clauses

## Performance Tips

1. **Index Relationships**: Create indexes on relationship columns for faster joins
2. **Limit Depth**: Keep recursion depth reasonable (< 10 levels)
3. **Filter Early**: Apply filters in the base case of recursive CTEs
4. **Partition Data**: Partition large tables by date or claim type

## Next Steps

- Integrate with real-time fraud detection pipelines
- Add machine learning models for fraud prediction
- Create automated alerts for high-risk claims
- Build dashboards for fraud analysts
- Implement graph algorithms using GraphX

## References

- [Databricks SQL Documentation](https://docs.databricks.com/sql/)
- [Recursive CTEs in Spark SQL](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-cte.html)
- [Delta Lake Documentation](https://docs.delta.io/)

## License

This demo is provided as-is for educational and demonstration purposes.

## Support

For issues or questions, please refer to the Databricks documentation or community forums.

