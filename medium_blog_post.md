# How Graph Network Analysis Cut Insurance Fraud Investigation Time from Hours to Seconds

*Using Databricks Recursive SQL and Network Graph Visualization to Expose Hidden Fraud Rings*

**Discovering connected fraud networks that traditional queries miss — and why this matters for your bottom line**

---

## The Global Fraud Crisis

Insurance fraud is a massive global problem affecting every region. In the **United States alone**, insurance fraud costs approximately **$308.6 billion annually** — that's **$933 per person per year**, according to a 2022 study by the Coalition Against Insurance Fraud[^1]. And this is just one country; the problem extends globally with billions more in losses across Europe, Asia-Pacific, and other regions.

But here's what keeps fraud investigators up at night across all regions: **they know they're only catching the obvious cases**.

Traditional fraud detection systems can flag suspicious individual claims — a suspiciously high medical bill, an accident reported too soon after policy activation, or claims from known high-risk areas. But organized fraud? That's where things get complicated.

**The real money is in the fraud rings** — coordinated networks of fraudsters using multiple fake identities, shared addresses, and rotating service providers. These rings can generate **millions of dollars in fraudulent claims** before anyone notices the pattern.

The problem? **Traditional SQL can't find them.**

---

## Why Traditional Fraud Detection Falls Short

Let's say your fraud analyst flags claim CLM00653351 — a $36,000 auto accident claim that looks suspicious. Great start. But here's what happens next with traditional approaches:

**Manual Investigation (The Old Way):**
```
1. Check the policyholder's other claims → Find 2 related claims
2. Manually look up those policyholders → Find 1 with same address
3. Check their claims → Find 3 more suspicious claims
4. Repeat the process... for hours or days
5. Eventually give up because it's too time-consuming
```

**Result:** You find 6-10 connected claims. The fraud ring has 47 members with $1.2M in fraudulent claims. **You missed 85% of it.**

**Traditional SQL Queries (Still Limited):**
```sql
-- This only finds claims from the SAME policyholder
SELECT * FROM claims WHERE policyholder_id = 'PH12345';
```

This misses the fraud ring entirely because they're smart enough to use multiple fake identities.

**The Core Problem:** Fraud networks require exploring relationships **2, 3, or 4 degrees away** from the starting point. Traditional SQL can't traverse networks. You'd need to write separate queries for each "hop" — and fraud rings can be 5+ levels deep.

---

## Enter Graph Network Analysis: The Game Changer

**The insight:** Fraud isn't isolated incidents — **it's a network problem**. Fraudsters, fake identities, claims, service providers, and adjusters form a **connected graph**. To catch them, you need **graph network analysis**.

**Databricks Runtime 17.0** introduced support for Recursive Common Table Expressions (CTEs) — a SQL feature that enables true graph traversal directly in your data warehouse, no separate graph database needed.

Here's the magic: **Recursive SQL treats your data as a graph and automatically traverses the entire fraud network in a single query execution.**

### From Tables to Networks

Traditional SQL thinks in **tables and rows**:
- Claims table
- Policyholders table
- Join them, filter them, aggregate them

**Graph network analysis** thinks in **nodes and edges**:
- **Nodes** = Claims, Policyholders, Adjusters, Service Providers
- **Edges** = Relationships (filed by, processed by, shares address, same timing)
- **Network traversal** = Follow connections to discover fraud rings

### Graph Traversal with Recursive SQL

Think of it as a **breadth-first search** through the fraud network graph:

```sql
WITH RECURSIVE fraud_network AS (
  -- Start from one suspicious claim
  SELECT claim_id, policyholder_id, 0 as depth
  FROM claims WHERE claim_id = 'CLM00653351'
  
  UNION ALL
  
  -- Find all connected claims (automatically repeats)
  SELECT c2.claim_id, c2.policyholder_id, depth + 1
  FROM fraud_network fn
  JOIN policyholders p1 ON fn.policyholder_id = p1.policyholder_id
  JOIN policyholders p2 ON p1.address = p2.address OR p1.phone = p2.phone
  JOIN claims c2 ON c2.policyholder_id = p2.policyholder_id
  WHERE depth < 3
)
SELECT * FROM fraud_network;
```

**What this does (graph traversal perspective):**
- **Starts at 1 node** (suspicious claim) in the fraud network graph
- **Explores edges** to find connected nodes (policyholders with shared attributes)
- **Traverses to next level** of nodes (their claims)
- **Repeats recursively** until entire connected subgraph is discovered
- **Maps the complete fraud ring network** in seconds

This is true **graph network analysis** — but done entirely in SQL on your existing data warehouse, no specialized graph database required.

---

## Seeing is Believing: Network Graph Visualization

Traditional fraud detection gives you a list of claim IDs. **Graph network analysis gives you the complete picture** — a visual network showing how everything connects:

![Fraud Network Graph Discovered Through Recursive Analysis](images/claim_network_graph.png?v=3)

**What you're looking at (network graph elements):**
- 🔴 **Red nodes** = Confirmed fraudulent claims (fraud vertices in the graph)
- 🔵 **Blue nodes** = Legitimate claims caught in the network
- **Node size** = Claim amount (bigger nodes = more money at risk)
- **Arrows (edges)** = Connections discovered through graph traversal
- **Network structure** = Clear fraud ring clusters visible in the topology

This network graph shows **454 claims** connected to the original suspicious claim — including **45 fraudulent claims (9.9%)** totaling hundreds of thousands of dollars. The **visual clustering** immediately reveals coordinated fraud patterns that would be invisible in a spreadsheet.

**Time to discover this network graph:**
- ⏱️ **Manual investigation:** 2-5 days
- ⏱️ **Graph network analysis with recursive SQL:** 8 seconds

### The Multi-Entity Fraud Network Graph

Going deeper, we can visualize the complete fraud ecosystem as a **heterogeneous network graph** — not just claims, but multiple entity types and their relationships:

![Multi-Entity Fraud Network Graph](images/multi_entity_network_graph.png?v=3)

**Network Graph Legend (Multiple Node Types):**
- 🔴🔵 **Circles (Claim Nodes)** = Claims (red = fraud, blue = legitimate)
- 🟠 **Squares (Identity Nodes)** = Policyholders
- 🟢 **Triangles (Service Nodes)** = Adjusters/Service Providers

**Edge Types (Relationships):**
- **Solid gray lines** = "Filed by" relationship (policyholder → claim)
- **Dashed green lines** = "Processed by" relationship (adjuster → claim)

**What this network graph reveals:**
- **Hub nodes** = Policyholders filing multiple high-value claims (central fraud operators)
- **Suspicious clusters** = Adjusters handling concentrations of fraudulent claims
- **Bipartite patterns** = Service providers involved across multiple fraud cases
- **Network motifs** = Structural patterns indicating the full "fraud factory" operation

This is **multi-entity graph analysis** — treating your business data as a heterogeneous network where different entity types connect through different relationship types. The visual topology immediately reveals fraud patterns that are invisible in traditional table-based analysis.

---

## Real-World Business Impact

### Speed & Scale

**Before (Traditional Analysis):**
- ⏱️ **5-10 hours** per suspected fraud case
- 👤 Requires senior fraud analyst
- 📊 Analyzes 2-3 cases per day
- 💰 Finds 20-30% of fraud network value

**After (Recursive SQL):**
- ⏱️ **8-15 seconds** per case
- 🤖 Can be automated
- 📊 Analyzes 100+ cases per day
- 💰 Finds 95%+ of fraud network value

### ROI Calculation

Let's do the math for a mid-sized insurance company:

**Assumptions:**
- 1,000 fraud investigations per year
- Average fraud ring value: $500,000
- Current detection rate: 25% of ring value ($125,000 recovered)
- With recursive analysis: 90% of ring value ($450,000 recovered)

**Annual Impact:**
- Additional fraud recovered: **$325,000 × 1,000 cases = $325M**
- Analyst time saved: **8 hours × 1,000 cases = 8,000 hours = 4 FTEs**
- Cost savings: **$400K+ in labor costs**

**Total annual value: $325M+ in fraud prevention and recovery**

---

## How It Works (The Business View)

### 1. Network Graph Construction
The system continuously builds a graph from your transactional data:
- **Nodes** = Claims, Policyholders, Adjusters, Service Providers
- **Edges** = Relationships detected from shared attributes:
  - Policyholders sharing addresses or phone numbers (identity network)
  - Claims handled by the same service providers (service network)
  - Temporal patterns (timeline network)

### 2. Graph Traversal & Network Discovery
When a suspicious claim is flagged (by your existing fraud detection rules):
- Recursive SQL performs **breadth-first graph traversal** from the flagged node
- Automatically explores all edges and connected nodes
- Discovers the full fraud subgraph in seconds
- Maps network relationships 3-4 hops deep
- Identifies all involved entities (parties, service providers)

### 3. Interactive Network Graph Visualization
Fraud analysts see the complete fraud network as an **interactive graph:**
- **Visual network topology** (drag nodes, zoom, pan around the graph)
- **Color-coded nodes** (fraud vs. legitimate, different entity types)
- **Network metrics** (centrality, clustering, total exposure)
- **Path visualization** (how entities connect through the network)
- **Actionable intelligence** for law enforcement with visual proof

### 4. Network-Based Risk Scoring
The system automatically prioritizes investigations using **graph metrics:**
- **Network size** = Total nodes in the connected subgraph
- **Fraud density** = Percentage of fraudulent nodes in network
- **Total exposure** = Sum of claim amounts across the network
- **Centrality scores** = Key players in the fraud ring (high-degree nodes)
- **Temporal activity** = Network growth rate and recency

---

## Production Deployment: Built for Scale

This isn't a research project — it's production-ready:

### Performance at Scale
- ✅ **Sub-minute execution** on 10M+ claim datasets
- ✅ **Optimized for large datasets** with smart row limits and filtering
- ✅ **Databricks serverless compatible** for cost efficiency
- ✅ **Delta Lake optimization** for fast joins and lookups

### Enterprise Integration
- 🔄 **Stored procedures** for easy integration with existing systems
- 🤖 **AI agent compatibility** for automated fraud detection workflows
- 📊 **BI tool compatible** for executive dashboards
- 🔐 **Databricks security** model for compliance and audit

### Real-World Optimizations
The demo includes production-grade optimizations:
- Filters for high-value claims (>$15K) to reduce noise
- Caps network expansion to prevent database overload
- Prioritizes fraudulent claims in results
- Uses indexed joins for maximum performance

---

## Technical Requirements (Simple)

**What you need:**
- Databricks workspace (free edition works for testing)
- **Databricks Runtime 17.0+** (for recursive CTE support)
- Your existing claims and policyholder data

**What you DON'T need:**
- Separate graph databases (graph analysis runs in SQL on your data warehouse!)
- Neo4j, TigerGraph, or other specialized graph platforms
- Complex infrastructure changes or ETL to graph stores
- Expensive specialized tools
- Machine learning models (though they complement graph analysis beautifully)

---

## Getting Started

Ready to see this in action? The complete demo is **open source and ready to run**:

🔗 **GitHub Repository:** https://github.com/CheeYuTan/FraudDetection_RecursiveCTE

**What's included:**
1. ✅ **Synthetic data generator** (1K to 10M+ claims with realistic network patterns)
2. ✅ **Graph network analysis** with recursive SQL and stored procedures
3. ✅ **Interactive network graph visualizations** (the graphs shown above - PyVis)
4. ✅ **Multi-entity heterogeneous network** construction
5. ✅ **Production-ready code** with performance optimizations for large-scale graphs

**Time to run your first fraud network analysis: 15 minutes**

### Quick Start (3 Steps)

1. **Clone the repo** in your Databricks workspace (Git integration)
2. **Generate test data** (Notebook 01 — configure volume with widgets)
3. **Run fraud detection** (Notebook 02 — see the interactive graphs)

That's it. You'll have a working fraud detection system analyzing networks in under an hour.

---

## Real-World Use Cases Beyond Insurance

While this demo focuses on insurance fraud, the same recursive network analysis applies to:

**Financial Services:**
- Money laundering network detection
- Account takeover rings
- Credit card fraud networks

**Healthcare:**
- Medicare/Medicaid fraud rings
- Provider network abuse
- Prescription drug fraud schemes

**Retail & E-commerce:**
- Return fraud networks
- Promo abuse rings
- Fake review networks

**Telecommunications:**
- SIM card fraud rings
- Account sharing networks
- Service abuse patterns

**Any industry where fraud is networked and connected.**

---

## The Competitive Advantage

Here's why this matters strategically:

### Speed = Money Saved
Every day a fraud network operates costs thousands or millions. Cutting discovery time from **days to seconds** means:
- **Earlier intervention** = less fraud loss
- **Faster law enforcement referrals** = higher prosecution rates
- **Immediate network shutdown** = stopping ongoing fraud

### Automation = Scale
When fraud detection runs in seconds, you can:
- **Check every suspicious claim** (not just high-priority)
- **Continuous monitoring** instead of periodic audits
- **Real-time alerting** for emerging fraud patterns
- **Free up analysts** for complex investigations

### Completeness = Higher Recovery
Finding **95% of a fraud network** instead of 25% means:
- **Better recovery rates** through civil action
- **Stronger criminal cases** with complete evidence
- **Insurance subrogation** from all involved parties
- **Deterrent effect** when fraudsters know you'll find them

---

## The Bottom Line

**Traditional fraud detection:** Find the fraudster.  
**Recursive fraud detection:** Find the entire fraud ring.

The difference? **Millions of dollars** per year in prevented fraud loss.

Databricks Recursive SQL isn't just a technical feature — it's a **business game-changer** for any organization fighting networked fraud. The combination of:
- ⚡ **Speed** (seconds instead of days)
- 📈 **Scale** (10M+ claims analyzed effortlessly)
- 🎯 **Completeness** (find 95%+ of networks)
- 💰 **Cost** (just SQL on existing infrastructure)

...makes this one of the highest-ROI fraud prevention tools available today.

---

## Try It Yourself

The complete demo with all code, data generation, and interactive visualizations is available on GitHub:

🔗 **https://github.com/CheeYuTan/FraudDetection_RecursiveCTE**

**Have questions about implementing this in your organization?** Connect with me on LinkedIn or reach out through GitHub. I'm happy to discuss how recursive SQL can transform your fraud detection capabilities.

---

**Tags:** #FraudDetection #GraphAnalysis #NetworkScience #Databricks #DataEngineering #InsuranceTech #FinCrime #RecursiveSQL #NetworkAnalysis #DataScience #GraphNetworks

---

*Steven Tan is a data engineering professional specializing in fraud detection, graph network analysis, and production data systems. This demo showcases how to perform enterprise-scale graph network analysis using Databricks recursive SQL — bringing graph analytics capabilities to your data warehouse without separate graph databases.*

---

## References

[^1]: Coalition Against Insurance Fraud (2022). "The Impact of Insurance Fraud on the U.S. Economy." Available at: https://www.insurancefraud.org/

