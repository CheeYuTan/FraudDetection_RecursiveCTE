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

### Step 1: Clone Repository in Databricks

1. In your Databricks workspace, go to **Workspace**
2. Click the dropdown arrow next to your username → **Git** → **Clone Repository**
3. Enter the repository URL: `https://github.com/CheeYuTan/FraudDetection_RecursiveCTE.git`
4. Choose a destination folder (e.g., `Fraud_Detection_Demo`)
5. Click **Clone**

This will clone the entire repository into your workspace, including all 3 notebooks:
- `notebooks/01_Dataset_Generation.py`
- `notebooks/02_Recursive_Fraud_Detection.py`
- `notebooks/03_Fraud_Analysis_Visualization.py`

### Step 2: Generate Dataset

1. Open the `notebooks/01_Dataset_Generation.py` notebook from the cloned repository
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

1. **notebooks/02_Recursive_Fraud_Detection.py**: Runs recursive queries to detect fraud networks
   - Configure widgets: Catalog and Schema (must match Step 2)
2. **notebooks/03_Fraud_Analysis_Visualization.py**: Provides analysis and visualizations
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

**Note:** This demo uses synthetic data with pre-labeled fraud rings and relationships for demonstration purposes. In a production system, these would be discovered from raw claim data.

### Policyholders
- Unique policyholders with realistic demographic data
- Geographic distribution across multiple states
- Policy start dates and contact information

### Claims
- Insurance claims with configurable fraud rate
- Multiple claim types: Auto, Home, Health, Property, Liability
- Claim amounts with realistic distributions (fraudulent claims tend to be higher value)
- Claim statuses: Pending, Approved, Denied, Under Review
- Incident dates and processing timelines

### Relationships (Production Approach)

In a real insurance system, relationships would be **derived from actual data patterns**, not pre-labeled. Common relationship sources include:

- **Shared attributes**: Same address, phone number, email, or adjuster
- **Similar patterns**: Same claim type, similar dates, similar amounts
- **Policyholder connections**: Claims from related policyholders (family members, business associates)
- **Geographic proximity**: Claims from same location or nearby areas
- **Temporal patterns**: Claims filed within short time windows
- **Service provider connections**: Same repair shop, medical provider, or attorney

The recursive queries then use these relationships to discover fraud networks.

### Fraud Rings (Production Approach)

In production, fraud rings are **discovered through recursive analysis**, not pre-labeled. The process:

1. Start with known fraudulent claims (from investigations, ML models, or manual review)
2. Use recursive CTEs to find all claims connected through relationships
3. Identify clusters/networks of connected claims
4. Analyze network characteristics to identify coordinated fraud patterns
5. Flag high-risk networks as potential fraud rings

The `fraud_ring_id` in this demo is synthetic for demonstration - in production, rings would be discovered and assigned through the recursive analysis process.

## Production Use: Adapting to Real Data

**Important:** This demo uses synthetic data with pre-labeled fraud rings and relationships. In production:

- **Relationships** must be derived from actual data patterns (shared addresses, phone numbers, adjusters, similar patterns, etc.)
- **Fraud rings** are discovered through recursive analysis, not pre-labeled
- Start with known fraud cases and use recursive CTEs to find connected claims

See [PRODUCTION_APPROACH.md](PRODUCTION_APPROACH.md) for detailed guidance on adapting this to real insurance claim data.

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


## Troubleshooting

### Issue: Recursive CTE not supported

**Solution**: Ensure you're using **Databricks Runtime 17.0 or later**. Recursive CTEs are only supported in Runtime 17.0 and later.

## License

This demo is provided as-is for educational and demonstration purposes.

## Support

For issues or questions, please refer to the Databricks documentation or community forums.

