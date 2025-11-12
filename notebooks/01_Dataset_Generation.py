# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Fraud Detection - Dataset Generation
# MAGIC 
# MAGIC This notebook generates synthetic insurance claim fraud data and writes it directly to Delta tables.
# MAGIC 
# MAGIC **Configure the widgets below to customize the dataset generation.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure Parameters

# COMMAND ----------

# Create widgets for user configuration
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "fraud_detection_demo", "Schema/Database Name")
dbutils.widgets.dropdown("volume_scale", "small", ["small", "medium", "large", "xlarge", "custom"], "Volume Scale")
dbutils.widgets.text("num_policyholders", "1000", "Number of Policyholders (used if custom)")
dbutils.widgets.text("num_claims", "5000", "Number of Claims (used if custom)")
dbutils.widgets.text("fraud_rate", "0.15", "Fraud Rate (0.0-1.0)")
dbutils.widgets.text("num_adjusters", "50", "Number of Adjusters")
dbutils.widgets.text("batch_size", "1000000", "Batch Size for Large Datasets (recommended: 1M-10M)")
dbutils.widgets.dropdown("overwrite_mode", "true", ["true", "false"], "Overwrite Existing Tables")

# COMMAND ----------

# Get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_scale = dbutils.widgets.get("volume_scale")
fraud_rate = float(dbutils.widgets.get("fraud_rate"))
num_adjusters = int(dbutils.widgets.get("num_adjusters"))
batch_size = int(dbutils.widgets.get("batch_size"))
overwrite_mode = dbutils.widgets.get("overwrite_mode") == "true"

# Define volume scales
volume_configs = {
    "small": {"policyholders": 1_000, "claims": 5_000},
    "medium": {"policyholders": 10_000, "claims": 50_000},
    "large": {"policyholders": 100_000, "claims": 1_000_000},
    "xlarge": {"policyholders": 1_000_000, "claims": 10_000_000},
    "custom": {"policyholders": int(dbutils.widgets.get("num_policyholders")), 
               "claims": int(dbutils.widgets.get("num_claims"))}
}

# Set volumes based on scale
config = volume_configs[volume_scale]
num_policyholders = config["policyholders"]
num_claims = config["claims"]

# Determine if we need batch processing (for large datasets)
use_batch_processing = num_claims > 100_000

# Display configuration
print("=" * 60)
print("Dataset Generation Configuration")
print("=" * 60)
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Volume Scale: {volume_scale.upper()}")
print(f"Number of Policyholders: {num_policyholders:,}")
print(f"Number of Claims: {num_claims:,}")
print(f"Fraud Rate: {fraud_rate*100:.1f}%")
print(f"Number of Adjusters: {num_adjusters}")
print(f"Batch Size: {batch_size:,}" if use_batch_processing else "Batch Processing: Not needed")
print(f"Overwrite Mode: {overwrite_mode}")
print(f"Processing Mode: {'Batch Processing' if use_batch_processing else 'Standard Processing'}")
print("=" * 60)

if num_claims >= 10_000_000:
    print("\n⚠️  WARNING: Generating 10M+ records. This may take significant time and resources.")
    print("   Consider using a larger cluster and monitoring progress.")
elif num_claims >= 1_000_000:
    print("\nℹ️  INFO: Generating 1M+ records. Using optimized batch processing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Import Libraries and Setup

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Create database/schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

print(f"Using catalog: {catalog}, schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Policyholders

# COMMAND ----------

print("Generating policyholders...")

def generate_policyholders_spark(n=1000, spark_session=spark):
    """Generate policyholder data using Spark for scalability"""
    from pyspark.sql.functions import udf, rand, lit, concat, format_string
    from pyspark.sql.types import StringType
    import random
    
    cities_states = [
        ('New York', 'NY'), ('Los Angeles', 'CA'), ('Chicago', 'IL'), 
        ('Houston', 'TX'), ('Phoenix', 'AZ'), ('Philadelphia', 'PA')
    ]
    
    # Create Spark DataFrame with range
    df = spark_session.range(n).select(
        (col("id") + 1).alias("id")
    )
    
    # Generate policyholder data using Spark functions
    cities_list = [c[0] for c in cities_states]
    states_list = [c[1] for c in cities_states]
    
    @udf(returnType=StringType())
    def get_city(seed):
        return random.choice(cities_list)
    
    @udf(returnType=StringType())
    def get_state(seed):
        return random.choice(states_list)
    
    policyholders_df = df.select(
        format_string("PH%06d", col("id")).alias("policyholder_id"),
        concat(lit("Policyholder_"), col("id")).alias("name"),
        (rand() * 50 + 25).cast("int").alias("age"),
        format_string("%d Main St", (rand() * 9900 + 100).cast("int")).alias("address"),
        get_city(rand()).alias("city"),
        get_state(rand()).alias("state"),
        format_string("%05d", (rand() * 90000 + 10000).cast("int")).alias("zip_code"),
        format_string("%03d-%03d-%04d", 
                     (rand() * 900 + 100).cast("int"),
                     (rand() * 900 + 100).cast("int"),
                     (rand() * 9000 + 1000).cast("int")).alias("phone"),
        concat(lit("policyholder"), col("id"), lit("@example.com")).alias("email"),
        (datetime.now() - timedelta(days=int(rand() * 1795 + 30))).strftime('%Y-%m-%d').alias("policy_start_date"),
        lit(False).alias("is_fraud_ring_member")
    )
    
    # Convert date column
    policyholders_df = policyholders_df.withColumn('policy_start_date', to_date(col('policy_start_date')))
    
    return policyholders_df

# Use Spark for large datasets, pandas for small ones
if num_policyholders > 10000:
    print(f"Using Spark-based generation for {num_policyholders:,} policyholders...")
    policyholders_df = generate_policyholders_spark(n=num_policyholders)
    policyholders_count = policyholders_df.count()
else:
    print(f"Using pandas-based generation for {num_policyholders:,} policyholders...")
    def generate_policyholders_pandas(n=1000):
        """Generate policyholder data using pandas (for small datasets)"""
        policyholders = []
        cities_states = [
            ('New York', 'NY'), ('Los Angeles', 'CA'), ('Chicago', 'IL'), 
            ('Houston', 'TX'), ('Phoenix', 'AZ'), ('Philadelphia', 'PA')
        ]
        
        for i in range(n):
            city, state = random.choice(cities_states)
            policyholders.append({
                'policyholder_id': f'PH{i+1:06d}',
                'name': f'Policyholder_{i+1}',
                'age': np.random.randint(25, 75),
                'address': f'{np.random.randint(100, 9999)} Main St',
                'city': city,
                'state': state,
                'zip_code': f'{np.random.randint(10000, 99999)}',
                'phone': f'{np.random.randint(100, 999)}-{np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}',
                'email': f'policyholder{i+1}@example.com',
                'policy_start_date': (datetime.now() - timedelta(days=np.random.randint(30, 1825))).strftime('%Y-%m-%d'),
                'is_fraud_ring_member': False
            })
        return pd.DataFrame(policyholders)
    
    policyholders_pdf = generate_policyholders_pandas(n=num_policyholders)
    policyholders_df = spark.createDataFrame(policyholders_pdf)
    policyholders_df = policyholders_df.withColumn('policy_start_date', to_date(col('policy_start_date')))
    policyholders_count = len(policyholders_pdf)

print(f"✓ Generated {policyholders_count:,} policyholders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generate Claims with Fraud Patterns

# COMMAND ----------

print("Generating claims with fraud patterns...")

if use_batch_processing:
    print(f"Using batch processing for {num_claims:,} claims (batch size: {batch_size:,})...")
    
    from pyspark.sql.functions import udf, rand, lit, concat, format_string, when, expr, least, col
    from pyspark.sql.types import StringType, BooleanType, DoubleType
    
    # Get policyholder IDs as broadcast variable for efficiency
    policyholder_ids_df = policyholders_df.select("policyholder_id").cache()
    policyholder_ids_list = [row.policyholder_id for row in policyholder_ids_df.collect()]
    
    # Create fraud rings (for large datasets, create a reasonable number)
    n_fraud_rings = max(1, min(1000, int(num_claims * fraud_rate / 10)))  # Cap at 1000 rings
    fraud_rings = []
    fraud_ring_members = set()
    
    for ring_id in range(n_fraud_rings):
        ring_size = np.random.randint(5, min(15, len(policyholder_ids_list)))
        ring_members = random.sample(policyholder_ids_list, ring_size)
        fraud_rings.append({
            'ring_id': f'RING{ring_id+1:06d}',
            'members': ring_members
        })
        fraud_ring_members.update(ring_members)
    
    # Broadcast fraud ring members for efficient lookup
    fraud_ring_members_bc = spark.sparkContext.broadcast(fraud_ring_members)
    fraud_rings_bc = spark.sparkContext.broadcast({m: f'RING{i+1:06d}' for i, ring in enumerate(fraud_rings) for m in ring['members']})
    
    # Generate claims in batches
    claim_types = ['Auto', 'Home', 'Health', 'Property', 'Liability']
    claim_statuses = ['Pending', 'Approved', 'Denied', 'Under Review']
    
    all_claims_dfs = []
    num_batches = (num_claims + batch_size - 1) // batch_size
    
    for batch_num in range(num_batches):
        batch_start = batch_num * batch_size
        batch_end = min((batch_num + 1) * batch_size, num_claims)
        batch_size_actual = batch_end - batch_start
        
        print(f"  Processing batch {batch_num + 1}/{num_batches} ({batch_size_actual:,} claims)...")
        
        # Create base DataFrame
        claims_batch = spark.range(batch_start, batch_end).select(
            (col("id") + 1).alias("claim_num")
        )
        
        # Generate claim data using Spark functions
        @udf(returnType=StringType())
        def get_policyholder_id(seed):
            return random.choice(policyholder_ids_list)
        
        @udf(returnType=StringType())
        def get_claim_type(seed):
            return random.choice(claim_types)
        
        @udf(returnType=StringType())
        def get_claim_status(seed):
            return random.choice(claim_statuses)
        
        @udf(returnType=BooleanType())
        def is_fraud_claim(ph_id):
            is_ring_member = ph_id in fraud_ring_members_bc.value
            if is_ring_member:
                return random.random() < 0.7
            else:
                return random.random() < 0.05
        
        @udf(returnType=StringType())
        def get_fraud_ring_id(ph_id):
            return fraud_rings_bc.value.get(ph_id)
        
        claims_batch_df = claims_batch.select(
            format_string("CLM%08d", col("claim_num")).alias("claim_id"),
            get_policyholder_id(rand()).alias("policyholder_id"),
            get_claim_type(rand()).alias("claim_type"),
            expr(f"date_sub(current_date(), cast(rand() * 729 + 1 as int))").alias("claim_date"),
            expr(f"date_sub(current_date(), cast(rand() * 60 + 1 as int))").alias("incident_date"),
            when(expr("rand() < 0.5"), 
                 least(expr("exp(9 + rand() * 1.5)"), lit(500000)))
            .otherwise(least(expr("exp(7 + rand() * 1.2)"), lit(100000)))
            .cast("double").alias("claim_amount"),
            get_claim_status(rand()).alias("claim_status"),
            concat(lit("Claim description for "), get_claim_type(rand()), lit(" incident")).alias("description"),
            is_fraud_claim(get_policyholder_id(rand())).alias("is_fraud"),
            get_fraud_ring_id(get_policyholder_id(rand())).alias("fraud_ring_id"),
            format_string("ADJ%03d", (rand() * num_adjusters + 1).cast("int")).alias("adjuster_id"),
            (rand() * 89 + 1).cast("int").alias("processing_days")
        )
        
        all_claims_dfs.append(claims_batch_df)
    
    # Union all batches
    print("Combining batches...")
    claims_df = all_claims_dfs[0]
    for df in all_claims_dfs[1:]:
        claims_df = claims_df.union(df)
    
    claims_df = claims_df.cache()
    claims_count = claims_df.count()
    fraud_count = claims_df.filter(col("is_fraud") == True).count()
    
    print(f"✓ Generated {claims_count:,} claims")
    print(f"✓ Created {len(fraud_rings)} fraud rings")
    print(f"✓ Fraudulent claims: {fraud_count:,} ({fraud_count/claims_count*100:.1f}%)")
    
else:
    # Use pandas for smaller datasets
    print(f"Using pandas-based generation for {num_claims:,} claims...")
    
    def generate_claims_pandas(policyholders_df, n_claims=5000, fraud_rate=0.15):
        """Generate insurance claims with fraud patterns"""
        claims = []
        policyholder_ids = [row.policyholder_id for row in policyholders_df.select("policyholder_id").collect()]
        policyholders_pdf = policyholders_df.toPandas()
        
        # Create fraud rings
        fraud_rings = []
        n_fraud_rings = max(1, int(n_claims * fraud_rate / 10))
        for ring_id in range(n_fraud_rings):
            ring_size = np.random.randint(5, min(15, len(policyholder_ids)))
            ring_members = random.sample(policyholder_ids, ring_size)
            fraud_rings.append({
                'ring_id': f'RING{ring_id+1:03d}',
                'members': ring_members
            })
            policyholders_pdf.loc[policyholders_pdf['policyholder_id'].isin(ring_members), 'is_fraud_ring_member'] = True
        
        claim_types = ['Auto', 'Home', 'Health', 'Property', 'Liability']
        claim_statuses = ['Pending', 'Approved', 'Denied', 'Under Review']
        
        for i in range(n_claims):
            policyholder_id = random.choice(policyholder_ids)
            is_ring_member = policyholders_pdf[policyholders_pdf['policyholder_id'] == policyholder_id]['is_fraud_ring_member'].values[0]
            
            is_fraud = False
            ring_id = None
            if is_ring_member:
                is_fraud = np.random.random() < 0.7
                for ring in fraud_rings:
                    if policyholder_id in ring['members']:
                        ring_id = ring['ring_id']
                        break
            else:
                is_fraud = np.random.random() < 0.05
            
            claim_date = datetime.now() - timedelta(days=np.random.randint(1, 730))
            
            if is_fraud:
                claim_amount = min(np.random.lognormal(mean=9, sigma=1.5), 500000)
                incident_date = claim_date - timedelta(days=np.random.randint(0, 30))
            else:
                claim_amount = min(np.random.lognormal(mean=7, sigma=1.2), 100000)
                incident_date = claim_date - timedelta(days=np.random.randint(0, 60))
            
            claims.append({
                'claim_id': f'CLM{i+1:08d}',
                'policyholder_id': policyholder_id,
                'claim_type': random.choice(claim_types),
                'claim_date': claim_date.strftime('%Y-%m-%d'),
                'incident_date': incident_date.strftime('%Y-%m-%d'),
                'claim_amount': round(claim_amount, 2),
                'claim_status': random.choice(claim_statuses),
                'description': f'Claim description for {random.choice(claim_types)} incident',
                'is_fraud': is_fraud,
                'fraud_ring_id': ring_id,
                'adjuster_id': f'ADJ{np.random.randint(1, num_adjusters+1):03d}',
                'processing_days': np.random.randint(1, 90)
            })
        
        return pd.DataFrame(claims), fraud_rings
    
    claims_pdf, fraud_rings = generate_claims_pandas(policyholders_df, n_claims=num_claims, fraud_rate=fraud_rate)
    claims_df = spark.createDataFrame(claims_pdf)
    claims_df = claims_df.withColumn('claim_date', to_date(col('claim_date'))) \
                         .withColumn('incident_date', to_date(col('incident_date')))
    
    claims_count = len(claims_pdf)
    fraud_count = claims_pdf['is_fraud'].sum()
    
    print(f"✓ Generated {claims_count:,} claims")
    print(f"✓ Created {len(fraud_rings)} fraud rings")
    print(f"✓ Fraudulent claims: {fraud_count:,} ({fraud_count/claims_count*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Claim Relationships

# COMMAND ----------

print("Generating claim relationships...")

if use_batch_processing:
    print("Using Spark-based relationship generation for large dataset...")
    # For large datasets, generate relationships using Spark
    # Focus on fraud ring connections (most important for fraud detection)
    from pyspark.sql.functions import col, rand, lit, when
    
    # Generate relationships within fraud rings using Spark
    fraud_ring_claims = claims_df.filter(col("fraud_ring_id").isNotNull())
    
    # Self-join to create relationships within same fraud ring
    relationships_df = fraud_ring_claims.alias("c1").join(
        fraud_ring_claims.alias("c2"),
        (col("c1.fraud_ring_id") == col("c2.fraud_ring_id")) & 
        (col("c1.claim_id") < col("c2.claim_id")) &  # Avoid duplicates
        (rand() < 0.3)  # 30% chance of connection
    ).select(
        col("c1.claim_id").alias("claim_id_1"),
        col("c2.claim_id").alias("claim_id_2"),
        lit("fraud_ring_connection").alias("relationship_type"),
        (rand() * 0.3 + 0.7).alias("strength")  # 0.7 to 1.0
    )
    
    # Limit relationships for very large datasets to avoid explosion
    if num_claims > 1_000_000:
        max_relationships = min(10_000_000, int(num_claims * 0.1))  # Max 10M or 10% of claims
        relationships_df = relationships_df.limit(max_relationships)
        print(f"  Limiting relationships to {max_relationships:,} for performance")
    
    relationships_df = relationships_df.cache()
    relationships_count = relationships_df.count()
    print(f"✓ Generated {relationships_count:,} relationships (fraud ring connections)")
    
else:
    # Use pandas for smaller datasets
    def generate_relationships_pandas(claims_df, fraud_rings):
        """Generate relationships between claims (for recursive queries)"""
        relationships = []
        claims_pdf = claims_df.toPandas()
        
        # Add relationships within fraud rings
        for ring in fraud_rings:
            ring_claims = claims_pdf[claims_pdf['fraud_ring_id'] == ring['ring_id']]
            claim_ids = ring_claims['claim_id'].tolist()
            
            # Create connections between claims in the same ring
            for i, claim1 in enumerate(claim_ids):
                for claim2 in claim_ids[i+1:]:
                    if np.random.random() < 0.3:  # 30% chance of connection
                        relationships.append({
                            'claim_id_1': claim1,
                            'claim_id_2': claim2,
                            'relationship_type': 'fraud_ring_connection',
                            'strength': np.random.uniform(0.7, 1.0)
                        })
        
        # Add relationships based on shared attributes (only for smaller datasets)
        if len(claims_pdf) < 10000:
            for _, claim in claims_pdf.iterrows():
                claim_date = pd.to_datetime(claim['claim_date'])
                similar_claims = claims_pdf[
                    (claims_pdf['claim_type'] == claim['claim_type']) &
                    (claims_pdf['claim_id'] != claim['claim_id']) &
                    (abs((pd.to_datetime(claims_pdf['claim_date']) - claim_date).dt.days) < 30) &
                    (abs(claims_pdf['claim_amount'] - claim['claim_amount']) < claim['claim_amount'] * 0.2)
                ]
                
                for _, similar in similar_claims.head(3).iterrows():
                    if np.random.random() < 0.1:  # 10% chance
                        relationships.append({
                            'claim_id_1': claim['claim_id'],
                            'claim_id_2': similar['claim_id'],
                            'relationship_type': 'similar_pattern',
                            'strength': np.random.uniform(0.3, 0.7)
                        })
        
        return pd.DataFrame(relationships)
    
    relationships_pdf = generate_relationships_pandas(claims_df, fraud_rings)
    relationships_df = spark.createDataFrame(relationships_pdf)
    relationships_count = len(relationships_pdf)
    print(f"✓ Generated {relationships_count:,} relationships")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Generate Adjusters

# COMMAND ----------

print("Generating adjusters...")

def generate_adjusters(n=50):
    """Generate insurance adjuster data"""
    adjusters = []
    for i in range(n):
        adjusters.append({
            'adjuster_id': f'ADJ{i+1:03d}',
            'name': f'Adjuster_{i+1}',
            'department': random.choice(['Auto', 'Home', 'Health', 'Property', 'Special Investigations']),
            'experience_years': np.random.randint(1, 20),
            'is_siu_member': np.random.random() < 0.2  # 20% are Special Investigations Unit
        })
    return pd.DataFrame(adjusters)

adjusters_pdf = generate_adjusters(n=num_adjusters)
adjusters_df = spark.createDataFrame(adjusters_pdf)

print(f"✓ Generated {len(adjusters_pdf):,} adjusters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write to Delta Tables

# COMMAND ----------

print("\nWriting data to Delta tables...")

write_mode = "overwrite" if overwrite_mode else "append"

# Write policyholders
policyholders_df.write.mode(write_mode).saveAsTable(f"{catalog}.{schema}.policyholders")
print(f"✓ Written to {catalog}.{schema}.policyholders")

# Write claims
claims_df.write.mode(write_mode).saveAsTable(f"{catalog}.{schema}.claims")
print(f"✓ Written to {catalog}.{schema}.claims")

# Write relationships
relationships_df.write.mode(write_mode).saveAsTable(f"{catalog}.{schema}.claim_relationships")
print(f"✓ Written to {catalog}.{schema}.claim_relationships")

# Write adjusters
adjusters_df.write.mode(write_mode).saveAsTable(f"{catalog}.{schema}.adjusters")
print(f"✓ Written to {catalog}.{schema}.adjusters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Enriched Views

# COMMAND ----------

print("\nCreating enriched views...")

# Create enriched claims view
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.claims_enriched AS
SELECT 
    c.claim_id,
    c.policyholder_id,
    p.name as policyholder_name,
    p.city,
    p.state,
    c.claim_type,
    c.claim_date,
    c.incident_date,
    c.claim_amount,
    c.claim_status,
    c.is_fraud,
    c.fraud_ring_id,
    c.adjuster_id,
    a.name as adjuster_name,
    a.department as adjuster_department,
    a.is_siu_member,
    c.processing_days
FROM {catalog}.{schema}.claims c
LEFT JOIN {catalog}.{schema}.policyholders p ON c.policyholder_id = p.policyholder_id
LEFT JOIN {catalog}.{schema}.adjusters a ON c.adjuster_id = a.adjuster_id
""")

print(f"✓ Created view {catalog}.{schema}.claims_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Dataset Summary

# COMMAND ----------

# Calculate summary statistics
if use_batch_processing:
    total_claims = claims_df.count()
    fraudulent_claims = claims_df.filter(col("is_fraud") == True).count()
    fraud_percentage = (fraudulent_claims / total_claims) * 100 if total_claims > 0 else 0
    total_amount = claims_df.agg({"claim_amount": "sum"}).collect()[0][0] or 0
    fraud_amount = claims_df.filter(col("is_fraud") == True).agg({"claim_amount": "sum"}).collect()[0][0] or 0
    fraud_amount_percentage = (fraud_amount / total_amount) * 100 if total_amount > 0 else 0
    relationships_count = relationships_df.count() if 'relationships_df' in locals() else 0
else:
    total_claims = claims_pdf.shape[0]
    fraudulent_claims = claims_pdf['is_fraud'].sum()
    fraud_percentage = (fraudulent_claims / total_claims) * 100
    total_amount = claims_pdf['claim_amount'].sum()
    fraud_amount = claims_pdf[claims_pdf['is_fraud']]['claim_amount'].sum()
    fraud_amount_percentage = (fraud_amount / total_amount) * 100 if total_amount > 0 else 0
    relationships_count = len(relationships_pdf) if 'relationships_pdf' in locals() else 0

print("=" * 60)
print("DATASET GENERATION COMPLETE")
print("=" * 60)
print(f"\n📊 Dataset Summary:")
print(f"  • Policyholders: {num_policyholders:,}")
print(f"  • Claims: {total_claims:,}")
print(f"  • Fraudulent Claims: {fraudulent_claims:,} ({fraud_percentage:.1f}%)")
print(f"  • Relationships: {relationships_count:,}")
print(f"  • Fraud Rings: {len(fraud_rings)}")
print(f"  • Adjusters: {num_adjusters}")
print(f"\n💰 Financial Summary:")
print(f"  • Total Claim Amount: ${total_amount:,.2f}")
print(f"  • Fraudulent Claim Amount: ${fraud_amount:,.2f}")
print(f"  • Fraud Amount Percentage: {fraud_amount_percentage:.1f}%")
print(f"\n📁 Tables Created:")
print(f"  • {catalog}.{schema}.policyholders")
print(f"  • {catalog}.{schema}.claims")
print(f"  • {catalog}.{schema}.claim_relationships")
print(f"  • {catalog}.{schema}.adjusters")
print(f"  • {catalog}.{schema}.claims_enriched (view)")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Preview Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview claims data
# MAGIC SELECT * FROM ${catalog}.${schema}.claims
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview fraud statistics
# MAGIC SELECT 
# MAGIC   claim_type,
# MAGIC   COUNT(*) as total_claims,
# MAGIC   SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
# MAGIC   ROUND(SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate
# MAGIC FROM ${catalog}.${schema}.claims
# MAGIC GROUP BY claim_type
# MAGIC ORDER BY fraud_rate DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Your dataset has been generated and is ready for analysis!
# MAGIC 
# MAGIC **Proceed to the next notebooks:**
# MAGIC 1. **01_Data_Ingestion.py** - (Optional) If you need to reload or transform data
# MAGIC 2. **02_Recursive_Fraud_Detection.py** - Run recursive queries to detect fraud networks
# MAGIC 3. **03_Fraud_Analysis_Visualization.py** - Analyze and visualize fraud patterns
# MAGIC 
# MAGIC **Note:** Make sure to update the catalog and schema names in the subsequent notebooks to match your configuration above.

