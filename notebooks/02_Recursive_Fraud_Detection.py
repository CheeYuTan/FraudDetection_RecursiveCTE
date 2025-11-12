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
# MAGIC ## Install Required Libraries
# MAGIC 
# MAGIC We need NetworkX for graph visualization. This only needs to be run once per cluster.

# COMMAND ----------

# Install required libraries for interactive graph visualization
%pip install networkx pyvis

# COMMAND ----------

# Import required PySpark functions
from pyspark.sql.functions import col, count, sum, when, lit

# Import visualization libraries
import networkx as nx
import pandas as pd
from pyvis.network import Network
import tempfile

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
    # Show sample of the network data
    print("Sample of discovered network:")
    network_df.show(20, truncate=False)
    
    # Network statistics
    print("\n📈 Network Statistics by Depth:")
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
# MAGIC ### Step 4d: Visualize the Fraud Network Graph
# MAGIC 
# MAGIC Now let's visualize the network as an actual graph to see how claims are connected through the recursive traversal!

# COMMAND ----------

if network_count > 0:
    print("🎨 Creating interactive network graph visualization...\n")
    
    # Convert to pandas for easier manipulation
    network_pd = network_df.toPandas()
    
    # Create PyVis network (undirected for cleaner visualization)
    net = Network(height='800px', width='100%', bgcolor='#222222', font_color='white', directed=True)
    
    # Configure physics for smooth animations
    net.set_options("""
    {
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -50,
          "centralGravity": 0.01,
          "springLength": 200,
          "springConstant": 0.08
        },
        "maxVelocity": 50,
        "solver": "forceAtlas2Based",
        "timestep": 0.35,
        "stabilization": {"iterations": 150}
      },
      "nodes": {
        "font": {"size": 12, "color": "white"}
      },
      "edges": {
        "color": {"inherit": true},
        "smooth": {"type": "continuous"}
      }
    }
    """)
    
    # Track nodes already added to avoid duplicates
    nodes_added = set()
    
    # Add nodes with rich attributes
    for idx, row in network_pd.iterrows():
        claim_id = row['claim_id']
        is_fraud = row['is_fraud']
        amount = row['claim_amount']
        claim_type = row['claim_type']
        depth = row['depth']
        
        if claim_id not in nodes_added:
            # Color based on fraud status
            color = '#ff4444' if is_fraud else '#4477ff'
            
            # Size based on claim amount
            size = max(15, min(50, amount / 1000))
            
            # Create hover tooltip
            title = f"""
            <b>Claim ID:</b> {claim_id}<br>
            <b>Type:</b> {claim_type}<br>
            <b>Amount:</b> ${amount:,.2f}<br>
            <b>Status:</b> {'🚨 FRAUD' if is_fraud else '✅ Legitimate'}<br>
            <b>Depth:</b> {depth} (steps from origin)<br>
            <b>Policyholder:</b> {row['policyholder_id']}
            """
            
            net.add_node(
                claim_id,
                label=claim_id[:10],
                color=color,
                size=size,
                title=title,
                shape='dot',
                borderWidth=2,
                borderWidthSelected=4
            )
            nodes_added.add(claim_id)
    
    # Add edges by parsing the path
    for idx, row in network_pd.iterrows():
        path_parts = row['path'].split(' -> ')
        for i in range(len(path_parts) - 1):
            net.add_edge(
                path_parts[i],
                path_parts[i + 1],
                color='#999999',
                width=2,
                arrows='to',
                smooth={'type': 'continuous'}
            )
    
    # Generate and display the network
    html_file = tempfile.NamedTemporaryFile(delete=False, suffix='.html', mode='w')
    net.save_graph(html_file.name)
    html_file.close()
    
    # Display in notebook
    with open(html_file.name, 'r') as f:
        html_content = f.read()
    
    displayHTML(html_content)
    
    print("\n✨ Interactive Graph Features:")
    print(f"  • 🖱️  Drag nodes to rearrange the network")
    print(f"  • 🔍 Scroll to zoom in/out")
    print(f"  • 👆 Hover over nodes to see claim details")
    print(f"  • 🎯 Click and drag to pan around")
    print(f"  • ⚡ Watch the physics simulation settle!")
    
    print(f"\n📊 Color Legend:")
    print(f"  • 🔴 Red nodes = Fraudulent claims")
    print(f"  • 🔵 Blue nodes = Legitimate claims")
    print(f"  • Node size = Claim amount (larger = higher value)")
    print(f"  • Arrows = Connection discovered through recursion")
    
    # Network metrics
    print(f"\n🔍 Network Metrics:")
    print(f"  • Total claims in network: {network_count}")
    print(f"  • Fraudulent claims: {total_fraud} ({total_fraud/network_count*100:.1f}%)")
    print(f"  • Starting claim: {target_claim_id}")
    
else:
    print("⚠️  No network to visualize - claim appears to be isolated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4e: Multi-Entity Network Graph (Advanced)
# MAGIC 
# MAGIC Let's create a more sophisticated network showing the relationships between:
# MAGIC - **Claims** (center of investigation)
# MAGIC - **Policyholders** (who filed the claims)
# MAGIC - **Service Providers** (adjusters who processed the claims)
# MAGIC 
# MAGIC This reveals the full ecosystem of a fraud network!

# COMMAND ----------

if network_count > 0:
    print("🎨 Creating multi-entity network graph...\n")
    
    # Get policyholder and adjuster information for the network
    network_with_details = network_df.toPandas()
    
    # Create a multi-partite graph
    G_multi = nx.Graph()
    
    # Track entity types for coloring
    claim_nodes = set()
    policyholder_nodes = set()
    adjuster_nodes = set()
    
    # Get unique policyholders and adjusters in the network
    policyholders_in_network = network_with_details['policyholder_id'].unique()
    
    # Get adjuster information
    adjusters_df = spark.sql(f"""
        SELECT DISTINCT c.adjuster_id, a.name as adjuster_name, a.department
        FROM {catalog}.{schema}.claims c
        INNER JOIN {catalog}.{schema}.adjusters a ON c.adjuster_id = a.adjuster_id
        WHERE c.claim_id IN ({','.join([f"'{cid}'" for cid in network_with_details['claim_id'].tolist()])})
    """).toPandas()
    
    # Add claim nodes
    for idx, row in network_with_details.iterrows():
        claim_id = row['claim_id']
        G_multi.add_node(claim_id, 
                        node_type='claim',
                        amount=row['claim_amount'],
                        is_fraud=row['is_fraud'],
                        claim_type=row['claim_type'])
        claim_nodes.add(claim_id)
        
        # Add policyholder node
        ph_id = row['policyholder_id']
        if ph_id not in G_multi:
            G_multi.add_node(ph_id, node_type='policyholder')
            policyholder_nodes.add(ph_id)
        
        # Connect claim to policyholder
        G_multi.add_edge(claim_id, ph_id, edge_type='filed_by')
    
    # Add adjuster nodes and connections
    for idx, row in adjusters_df.iterrows():
        adj_id = row['adjuster_id']
        if adj_id not in G_multi:
            G_multi.add_node(adj_id, 
                           node_type='adjuster',
                           department=row['department'])
            adjuster_nodes.add(adj_id)
        
        # Connect adjusters to their claims
        adj_claims = network_with_details[network_with_details['claim_id'].isin(
            spark.sql(f"""
                SELECT claim_id 
                FROM {catalog}.{schema}.claims 
                WHERE adjuster_id = '{adj_id}'
                AND claim_id IN ({','.join([f"'{cid}'" for cid in network_with_details['claim_id'].tolist()])})
            """).toPandas()['claim_id'].tolist() if spark.sql(f"""
                SELECT claim_id 
                FROM {catalog}.{schema}.claims 
                WHERE adjuster_id = '{adj_id}'
                AND claim_id IN ({','.join([f"'{cid}'" for cid in network_with_details['claim_id'].tolist()])})
            """).count() > 0 else []
        )]
        
        for claim_id in adj_claims['claim_id']:
            G_multi.add_edge(adj_id, claim_id, edge_type='processed_by')
    
    # Create visualization with different layouts
    plt.figure(figsize=(20, 16))
    
    # Use spring layout for better visualization
    pos = nx.spring_layout(G_multi, k=3, iterations=100, seed=42)
    
    # Prepare node colors and sizes by type
    node_colors = []
    node_sizes = []
    node_shapes = []
    
    for node in G_multi.nodes():
        node_data = G_multi.nodes[node]
        
        if node_data['node_type'] == 'claim':
            # Claims: Red for fraud, Blue for legitimate
            if node_data['is_fraud']:
                node_colors.append('#ff4444')  # Red
            else:
                node_colors.append('#4477ff')  # Blue
            # Size based on claim amount
            node_sizes.append(max(500, min(4000, node_data['amount'] / 50)))
        elif node_data['node_type'] == 'policyholder':
            # Policyholders: Orange
            node_colors.append('#ff9933')
            node_sizes.append(800)
        else:  # adjuster
            # Adjusters: Green
            node_colors.append('#44dd44')
            node_sizes.append(1200)
    
    # Draw nodes
    nx.draw_networkx_nodes(
        G_multi, pos,
        node_color=node_colors,
        node_size=node_sizes,
        alpha=0.85,
        edgecolors='black',
        linewidths=2.5
    )
    
    # Draw edges with different styles
    claim_ph_edges = [(u, v) for u, v, d in G_multi.edges(data=True) if d.get('edge_type') == 'filed_by']
    claim_adj_edges = [(u, v) for u, v, d in G_multi.edges(data=True) if d.get('edge_type') == 'processed_by']
    other_edges = [(u, v) for u, v, d in G_multi.edges(data=True) if 'edge_type' not in d]
    
    # Claim to policyholder edges (solid)
    nx.draw_networkx_edges(
        G_multi, pos,
        edgelist=claim_ph_edges,
        edge_color='#666666',
        width=2,
        alpha=0.6,
        style='solid'
    )
    
    # Claim to adjuster edges (dashed)
    nx.draw_networkx_edges(
        G_multi, pos,
        edgelist=claim_adj_edges,
        edge_color='#44dd44',
        width=2,
        alpha=0.5,
        style='dashed'
    )
    
    # Other edges (fraud network connections)
    nx.draw_networkx_edges(
        G_multi, pos,
        edgelist=other_edges,
        edge_color='#999999',
        width=1,
        alpha=0.3
    )
    
    # Add labels
    labels = {}
    for node in G_multi.nodes():
        node_data = G_multi.nodes[node]
        if node_data['node_type'] == 'claim':
            labels[node] = node[:10]  # Shortened claim ID
        elif node_data['node_type'] == 'policyholder':
            labels[node] = f"PH\n{node[-4:]}"  # Last 4 digits
        else:  # adjuster
            labels[node] = f"ADJ\n{node[-3:]}"  # Last 3 digits
    
    nx.draw_networkx_labels(
        G_multi, pos,
        labels,
        font_size=7,
        font_weight='bold',
        font_color='white'
    )
    
    # Add legend
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    legend_elements = [
        Patch(facecolor='#ff4444', edgecolor='black', label='Fraudulent Claim'),
        Patch(facecolor='#4477ff', edgecolor='black', label='Legitimate Claim'),
        Patch(facecolor='#ff9933', edgecolor='black', label='Policyholder'),
        Patch(facecolor='#44dd44', edgecolor='black', label='Adjuster/Service Provider'),
        Line2D([0], [0], color='#666666', linewidth=2, label='Filed By'),
        Line2D([0], [0], color='#44dd44', linewidth=2, linestyle='--', label='Processed By')
    ]
    plt.legend(handles=legend_elements, loc='upper right', fontsize=12, framealpha=0.95)
    
    plt.title(f"Multi-Entity Fraud Network - Starting from Claim {target_claim_id}\n"
              f"Claims: {len(claim_nodes)} | Policyholders: {len(policyholder_nodes)} | Adjusters: {len(adjuster_nodes)}",
              fontsize=18, fontweight='bold', pad=20)
    plt.axis('off')
    plt.tight_layout()
    
    # Display the graph
    display(plt.show())
    
    print("\n📊 Multi-Entity Graph Interpretation:")
    print(f"  • Red circles = Fraudulent claims")
    print(f"  • Blue circles = Legitimate claims")
    print(f"  • Orange circles = Policyholders who filed the claims")
    print(f"  • Green circles = Adjusters/Service providers who processed claims")
    print(f"  • Solid lines = 'Filed by' relationships (claim → policyholder)")
    print(f"  • Dashed green lines = 'Processed by' relationships (claim → adjuster)")
    print(f"  • Circle size = Claim amount (for claim nodes)")
    print(f"\n💡 This shows the full fraud ecosystem:")
    print(f"  • How multiple claims connect through shared policyholders")
    print(f"  • Which adjusters handled suspicious claims")
    print(f"  • Potential coordination between entities")
    
else:
    print("⚠️  No network to visualize - claim appears to be isolated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Production Usage Guide
# MAGIC 
# MAGIC ### Recommended Workflow for Fraud Investigation:
# MAGIC 
# MAGIC 1. **Identify Suspicious Claims**: Use ML models, rules engines, or manual reports
# MAGIC 2. **Understand Relationships**: Call `get_claim_relationships(claim_id)` to see direct connections
# MAGIC 3. **Discover Network**: Call `discover_fraud_network(claim_id, depth)` to reveal the full fraud ring using recursive CTEs
# MAGIC 4. **Visualize**: Use the network graphs to understand the fraud ecosystem
# MAGIC 5. **Take Action**: Prioritize investigation of connected high-value or fraudulent claims
# MAGIC 
# MAGIC ### Performance Tips:
# MAGIC 
# MAGIC - **Start with `max_depth=2` or `3`**: Recursion grows exponentially, so start shallow
# MAGIC - **Investigate specific claims**: These procedures are designed for targeted investigation, not bulk processing
# MAGIC - **Adjust based on results**: If you find a large network, reduce depth; if isolated, try increasing
# MAGIC - **Use visualizations**: The multi-entity graph reveals patterns not visible in tables
# MAGIC 
# MAGIC ### Integration with Agentic Systems:
# MAGIC 
# MAGIC These stored procedures can be called by AI agents for automated fraud detection:
# MAGIC 
# MAGIC ```python
# MAGIC # Agent receives suspicious claim ID from alerting system
# MAGIC relationships = spark.sql(f"CALL get_claim_relationships('{claim_id}')")
# MAGIC network = spark.sql(f"CALL discover_fraud_network('{claim_id}', 3)")
# MAGIC 
# MAGIC # Agent analyzes network metrics
# MAGIC fraud_rate = network.filter("is_fraud = true").count() / network.count()
# MAGIC 
# MAGIC # Agent takes action based on fraud concentration
# MAGIC if fraud_rate > 0.5:
# MAGIC     escalate_to_investigators(claim_id, network)
# MAGIC ```
# MAGIC 
# MAGIC ### What Makes This Demo Powerful:
# MAGIC 
# MAGIC ✅ **Recursive CTEs**: Traverse complex fraud networks efficiently  
# MAGIC ✅ **On-Demand Relationships**: No pre-computed tables needed  
# MAGIC ✅ **Multi-Entity Visualization**: See claims, policyholders, and service providers together  
# MAGIC ✅ **Production-Ready**: Stored procedures can be deployed and used by agents  
# MAGIC ✅ **Scalable**: Optimized for Databricks with depth limits and targeted queries

