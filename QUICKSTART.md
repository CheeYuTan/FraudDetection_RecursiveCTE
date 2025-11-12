# Quick Start Guide

Get up and running with the Fraud Detection Demo in 5 minutes!

## Step 1: Clone Repository in Databricks (1 minute)

1. In Databricks workspace, go to **Workspace**
2. Click dropdown next to your username → **Git** → **Clone Repository**
3. Enter: `https://github.com/CheeYuTan/FraudDetection_RecursiveCTE.git`
4. Choose destination folder (e.g., `Fraud_Detection_Demo`)
5. Click **Clone**

This clones all notebooks automatically!

## Step 2: Generate Dataset (2 minutes)

1. Open `notebooks/01_Dataset_Generation.py` from the cloned repository
2. Configure widgets:
   - Catalog: `main` (or your catalog)
   - Schema: `fraud_detection_demo` (or your schema)
   - Adjust other parameters as needed
3. Run the notebook - data is generated directly in Delta tables!

## Step 3: Run Fraud Detection Analysis (1 minute)

1. **notebooks/02_Recursive_Fraud_Detection.py** - Detect fraud networks (configure widgets!)
2. **notebooks/03_Fraud_Analysis_Visualization.py** - Show analysis (configure widgets!)

**Important:** Configure the catalog and schema widgets in each notebook to match your setup!

## What You'll See

### Fraud Network Detection
- Networks of connected fraudulent claims
- Policyholder fraud rings
- Suspicious claim chains

### Key Metrics
- Configurable dataset volume (from small to 100M+ records)
- Configurable fraud rate (default 15%)
- Multiple fraud rings with coordinated patterns
- Fraudulent claims typically have higher claim amounts

### Recursive Queries
The demo showcases:
- **Recursive CTEs** to traverse claim relationships
- **Network analysis** to find connected fraud
- **Pattern detection** across time and amounts

## Next Steps

- Modify fraud rate using widgets in `01_Dataset_Generation.py`
- Adjust recursion depth in queries
- Add your own analysis queries
- Integrate with real data sources

## Troubleshooting

**"Recursive CTE not supported"**
- Use **Databricks Runtime 17.0 or later** (required)

**"Out of memory"**
- Reduce dataset size (use widgets in `01_Dataset_Generation.py`)
- Lower recursion depth
- Add more filters

## Need Help?

See the full [README.md](README.md) for detailed documentation.
