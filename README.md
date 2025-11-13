# Insurance Fraud Detection Demo - Databricks Recursive Capabilities

This demo showcases how to use Databricks recursive SQL capabilities to detect insurance claim fraud through network analysis and pattern recognition.

## Overview

The demo includes:
- **Synthetic Dataset**: Realistic insurance claim data with fraud patterns and networks
- **Recursive SQL Queries**: Advanced queries to detect fraud rings and connected claims
- **Interactive Network Visualizations**: Stunning graph visualizations of fraud networks discovered through recursion
- **Stored Procedures**: Reusable fraud detection tools for production and agentic systems
- **Network Analysis**: Identify policyholder networks and suspicious claim chains

## 🎨 Interactive Visualizations

### Claim Network Graph
Interactive network showing how fraudulent claims connect through shared policyholders:

**Features:**
- 🖱️ **Drag nodes** to rearrange the network
- 🔍 **Scroll to zoom** in/out
- 👆 **Hover over nodes** to see claim details
- 🎯 **Click and drag** to pan around
- ⚡ **Physics simulation** settles automatically

**Color Legend:**
- 🔴 **Red nodes** = Fraudulent claims
- 🔵 **Blue nodes** = Legitimate claims
- **Node size** = Claim amount (larger = higher value)
- **Arrows** = Connection discovered through recursion

**Network Metrics Shown:**
- Total claims in network
- Fraudulent claims count and percentage
- Starting claim identification

### Multi-Entity Network Graph
Advanced visualization showing the complete fraud ecosystem with multiple entity types:

**Entity Types:**
- 🔴🔵 **Circles** = Claims (red=fraud, blue=legitimate)
- 🟠 **Squares** = Policyholders
- 🟢 **Triangles** = Adjusters/Service Providers

**Relationship Types:**
- **Solid gray lines** = Filed by (policyholder → claim)
- **Dashed green lines** = Processed by (adjuster → claim)

**What to Look For:**
- Clusters of red (fraud) nodes = potential fraud rings
- Policyholders connected to many claims = suspicious activity
- Adjusters linked to multiple fraud claims = investigation needed
- Dense connections = coordinated fraud patterns

## Project Structure

```
.
├── README.md                          # This file
└── notebooks/                         # Databricks notebooks
    ├── 01_Dataset_Generation.py      # ⭐ Generate dataset directly in Databricks
    └── 02_Recursive_Fraud_Detection.py  # Recursive fraud detection with network visualization
```

**Clean and minimal!** Just 2 notebooks with everything you need:
1. Generate data
2. Detect fraud networks with recursive CTEs and visualize them

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

This will clone the entire repository into your workspace, including both notebooks:
- `notebooks/01_Dataset_Generation.py`
- `notebooks/02_Recursive_Fraud_Detection.py`

### Step 2: Generate Dataset

1. Open the `notebooks/01_Dataset_Generation.py` notebook from the cloned repository
2. Configure the widgets at the top of the notebook:
   - **Catalog**: Your catalog name (default: `dbdemos_steventan`)
   - **Schema**: Your schema/database name (default: `frauddetection_recursivecte`)
   - **Volume Scale**: Choose from small, medium, large, xlarge, or custom
   - **Number of Policyholders**: Used if custom scale (default: 1000)
   - **Number of Claims**: Used if custom scale (default: 5000)
   - **Fraud Rate**: Default 0.15 (15%)
   - **Number of Adjusters**: Default 50
   - **Batch Size**: For large datasets (default: 1,000,000)
   - **Overwrite Mode**: true/false
3. Run the notebook - it will generate the data and write directly to Delta tables

### Step 3: Run Fraud Detection & Visualization

1. Open **notebooks/02_Recursive_Fraud_Detection.py**
2. Configure widgets: Catalog and Schema (must match Step 2)
3. Run the notebook to:
   - Create reusable stored procedures for fraud detection
   - Follow an interactive fraud investigation story
   - Discover fraud networks using recursive CTEs
   - **Visualize the fraud network as an interactive graph** showing:
     - Red nodes = Fraudulent claims
     - Blue nodes = Legitimate claims
     - Node size = Claim amount
     - Arrows = Connections discovered through recursion
   - Analyze network statistics and patterns

**Important:** All notebooks use widgets for catalog and schema configuration. Make sure to set the same catalog and schema values across both notebooks!

## Key Features

### 🎯 Interactive Fraud Investigation Story

The demo walks you through a realistic fraud investigation workflow:
1. **Identify** suspicious claims (high-value fraudulent claims)
2. **Understand** direct relationships (why claims are connected)
3. **Discover** the full fraud network using recursive CTEs
4. **Visualize** the network as an interactive graph

### 🕸️ Network Graph Visualization

The demo includes a stunning network graph visualization that shows:
- **Nodes**: Each claim in the fraud network
  - Red = Fraudulent claims
  - Blue = Legitimate claims
  - Size = Claim amount (larger = higher value)
- **Edges**: Connections discovered through recursive traversal
- **Arrows**: Direction of network exploration
- **Network metrics**: Diameter, average degree, total impact

This makes the "recursion magic" visible and compelling!

### 🔄 Recursive Fraud Network Detection

The demo uses recursive Common Table Expressions (CTEs) to:
- Start from a suspicious claim
- Find all claims connected through shared policyholders (same address/phone)
- Traverse the network recursively to reveal fraud rings
- Calculate network statistics and fraud concentration

### 📦 Reusable Stored Procedures

Two production-ready stored procedures:
1. **`discover_fraud_network(claim_id, max_depth)`** - Main recursive CTE demonstration
2. **`get_claim_relationships(claim_id)`** - Shows direct relationships and connection types

These can be called interactively or integrated with AI agents for automated fraud detection.

## Dataset Details

**Note:** This demo generates synthetic data with discoverable fraud patterns. Relationships are derived from shared attributes (addresses, phone numbers, adjusters, similar patterns), and fraud rings are discovered through recursive analysis, just like in production.

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
- Each claim is assigned to an adjuster (insurance professional who investigates the claim)

### Adjusters
- Insurance adjusters who investigate and process claims
- Departments: Auto, Home, Health, Property, Special Investigations
- Some adjusters are members of the Special Investigations Unit (SIU) - specialized fraud investigators
- Used to detect service provider connections: claims handled by the same adjuster within a time window are related
- Important for fraud detection: patterns of multiple suspicious claims handled by the same adjuster can indicate coordinated fraud

### Relationships

Relationships are derived from data patterns and fall into three categories:

1. **Policyholder connections**: Claims from related policyholders (same address, phone number, etc.)
2. **Temporal patterns**: Claims filed within short time windows (within 30 days) with similar characteristics
3. **Service provider connections**: Claims handled by the same service provider (adjuster, repair shop, medical provider, attorney, etc.) within 90 days

The recursive queries use these relationships to discover fraud networks.

## How It Works

### The Fraud Investigation Workflow

The demo demonstrates a realistic fraud investigation process:

1. **Data Generation (Notebook 01)**
   - Generate synthetic insurance claims with realistic patterns
   - Include fraud indicators (higher amounts, suspicious timing)
   - Create relationships through shared attributes (address, phone, adjuster)

2. **Fraud Detection (Notebook 02)**
   - Identify suspicious high-value claims
   - Use stored procedures to discover relationships
   - **Apply recursive CTEs** to traverse the entire fraud network
   - Visualize the network as an interactive graph
   - Analyze network statistics and fraud concentration

### Fraud Rings (Discovery-Based)

Fraud rings are **discovered through recursive analysis** from the generated data:

1. Start with known fraudulent claims (marked with `is_fraud = true`)
2. Use recursive CTEs to find all claims connected through relationships
3. Identify clusters/networks of connected claims
4. Analyze network characteristics to identify coordinated fraud patterns
5. Flag high-risk networks as potential fraud rings

The recursive queries in `02_Recursive_Fraud_Detection.py` demonstrate this discovery process.

## Adapting to Real Data

This demo uses the same approach as production systems:

- **Relationships** are derived from data patterns (policyholder connections, temporal patterns, service provider connections)
- **Fraud rings** are discovered through recursive analysis from relationships
- Start with known fraud cases and use recursive CTEs to find connected claims

To adapt to your real data, ensure your relationship generation captures:
1. Policyholder connections (shared addresses, phone numbers, family/business relationships)
2. Temporal patterns (claims filed within short time windows)
3. Service provider connections (same adjusters, repair shops, medical providers, attorneys)

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

### Using Stored Procedures (Recommended)

For better reusability and performance, you can create stored procedures from the SQL scripts:

1. Run `sql_scripts/fraud_detection_stored_procedures.sql` in Databricks SQL
2. Use stored procedures instead of inline queries:
   ```sql
   -- Find fraud networks
   CALL discover_fraud_networks(max_depth => 5, min_network_size => 2);
   
   -- BFS from a specific claim
   CALL fraud_network_bfs(start_claim_id => 'CLM00000001', max_depth => 3);
   
   -- Get relationships for a claim (on-demand, no pre-generation needed)
   CALL get_claim_relationships(target_claim_id => 'CLM00000001');
   
   -- Discover networks with on-demand relationship computation
   CALL discover_fraud_networks_ondemand(max_depth => 5);
   ```

Stored procedures can also be used as tools in agentic systems (see [this article](https://medium.com/dbsql-sme-engineering/graph-analytics-with-dbsql-stored-procedures-and-agents-5e53bad68032) for examples).

### Modifying Recursion Depth

In the recursive queries, adjust the depth limit:
```sql
WHERE fn.depth < 5  -- Change to desired depth
```

Or use stored procedures with parameters:
```sql
CALL discover_fraud_networks(max_depth => 10, min_network_size => 3);
```

## Troubleshooting

### Issue: Recursive CTE not supported

**Solution**: Ensure you're using **Databricks Runtime 17.0 or later**. Recursive CTEs are only supported in Runtime 17.0 and later.

## License

This demo is provided as-is for educational and demonstration purposes.

## Support

For issues or questions, please refer to the Databricks documentation or community forums.

