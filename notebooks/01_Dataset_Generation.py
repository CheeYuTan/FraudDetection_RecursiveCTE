# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Fraud Detection - Dataset Generation
# MAGIC 
# MAGIC This notebook generates synthetic insurance claim fraud data and writes it directly to Delta tables.
# MAGIC 
# MAGIC **Requirements:**
# MAGIC - Databricks Runtime 17.0 or later
# MAGIC 
# MAGIC **Configure the widgets below to customize the dataset generation.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure Parameters

# COMMAND ----------

# Create widgets for user configuration
dbutils.widgets.text("catalog", "dbdemos_steventan", "Catalog Name")
dbutils.widgets.text("schema", "frauddetection_recursivecte", "Schema/Database Name")
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

# Check Databricks Runtime version
try:
    runtime_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    print(f"Databricks Runtime: {runtime_version}")
    # Extract version number (e.g., "17.0" from "17.0.x-scala2.12")
    import re
    version_match = re.search(r'(\d+)\.(\d+)', runtime_version)
    if version_match:
        major, minor = int(version_match.group(1)), int(version_match.group(2))
        if major < 17 or (major == 17 and minor < 0):
            print("⚠️  WARNING: This project requires Databricks Runtime 17.0 or later.")
            print("   Recursive CTEs (used in other notebooks) are only available in Runtime 17.0+.")
    else:
        print("⚠️  Unable to determine runtime version. Please ensure you're using Runtime 17.0+")
except Exception as e:
    print("⚠️  Could not check runtime version. Please ensure you're using Databricks Runtime 17.0+")

# Create database/schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
# Use catalog and schema separately
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Using catalog: {catalog}, schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Policyholders

# COMMAND ----------

print("Generating policyholders...")

def generate_policyholders_spark(n=1000, spark_session=spark):
    """Generate policyholder data using Spark for scalability"""
    from pyspark.sql.functions import udf, rand, lit, concat, format_string, expr, col
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
        expr(f"date_sub(current_date(), cast(rand() * 1795 + 30 as int))").alias("policy_start_date")
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
                'policy_start_date': (datetime.now() - timedelta(days=np.random.randint(30, 1825))).strftime('%Y-%m-%d')
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
    
    from pyspark.sql.functions import udf, rand, lit, concat, format_string, when, expr, least, col, array, element_at
    from pyspark.sql.types import StringType, BooleanType, DoubleType
    
    # Get policyholder count for random selection (avoid collecting to Python list)
    policyholder_count = policyholders_df.count()
    
    # Add row number to policyholders for efficient random selection
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, monotonically_increasing_id
    
    # Create indexed policyholders view for efficient random selection
    policyholders_indexed = policyholders_df.select(
        col("policyholder_id"),
        (monotonically_increasing_id() % policyholder_count).alias("ph_index")
    )
    policyholders_indexed.createOrReplaceTempView("temp_policyholders_indexed")
    
    # Generate claims in batches and write incrementally to avoid memory issues
    claim_types = ['Auto', 'Home', 'Health', 'Property', 'Liability']
    claim_statuses = ['Pending', 'Approved', 'Denied', 'Under Review']
    
    num_batches = (num_claims + batch_size - 1) // batch_size
    
    # Write first batch to create the table
    batch_start = 0
    batch_end = min(batch_size, num_claims)
    batch_size_actual = batch_end - batch_start
    
    print(f"  Processing batch 1/{num_batches} ({batch_size_actual:,} claims)...")
    
    # Drop table first if overwrite mode to avoid schema conflicts
    if overwrite_mode:
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.claims")
    
    # Generate claims using join with indexed policyholders (efficient random selection)
    claims_batch_df = spark.range(batch_start, batch_end).select(
        (col("id") + 1).alias("claim_num"),
        (rand() * policyholder_count).cast("int").alias("ph_index")
    ).join(
        policyholders_indexed,
        "ph_index",
        "left"
    ).select(
        format_string("CLM%08d", col("claim_num")).alias("claim_id"),
        col("policyholder_id"),
        element_at(array([lit(ct) for ct in claim_types]), 
                   (rand() * len(claim_types) + 1).cast("int")).alias("claim_type"),
        expr(f"date_sub(current_date(), cast(rand() * 729 + 1 as int))").alias("claim_date"),
        expr(f"date_sub(current_date(), cast(rand() * 60 + 1 as int))").alias("incident_date"),
        when(expr("rand() < 0.5"), 
             least(expr("exp(9 + rand() * 1.5)"), lit(500000)))
        .otherwise(least(expr("exp(7 + rand() * 1.2)"), lit(100000)))
        .cast("double").alias("claim_amount"),
        element_at(array([lit(cs) for cs in claim_statuses]), 
                   (rand() * len(claim_statuses) + 1).cast("int")).alias("claim_status"),
        concat(lit("Claim description for "), 
               element_at(array([lit(ct) for ct in claim_types]), 
                         (rand() * len(claim_types) + 1).cast("int")), 
               lit(" incident")).alias("description"),
        (rand() < lit(fraud_rate)).cast(BooleanType()).alias("is_fraud"),
        format_string("ADJ%03d", (rand() * num_adjusters + 1).cast("int")).alias("adjuster_id"),
        ((rand() * 89 + 1).cast("int")).alias("processing_days")
    )
    
    # Write first batch to create table
    write_mode = "overwrite" if overwrite_mode else "errorifexists"
    claims_batch_df.write.mode(write_mode).saveAsTable(f"{catalog}.{schema}.claims")
    
    # Process remaining batches and append
    for batch_num in range(1, num_batches):
        batch_start = batch_num * batch_size
        batch_end = min((batch_num + 1) * batch_size, num_claims)
        batch_size_actual = batch_end - batch_start
        
        print(f"  Processing batch {batch_num + 1}/{num_batches} ({batch_size_actual:,} claims)...")
        
        claims_batch_df = spark.range(batch_start, batch_end).select(
            (col("id") + 1).alias("claim_num"),
            (rand() * policyholder_count).cast("int").alias("ph_index")
        ).join(
            policyholders_indexed,
            "ph_index",
            "left"
        ).select(
            format_string("CLM%08d", col("claim_num")).alias("claim_id"),
            col("policyholder_id"),
            element_at(array([lit(ct) for ct in claim_types]), 
                       (rand() * len(claim_types) + 1).cast("int")).alias("claim_type"),
            expr(f"date_sub(current_date(), cast(rand() * 729 + 1 as int))").alias("claim_date"),
            expr(f"date_sub(current_date(), cast(rand() * 60 + 1 as int))").alias("incident_date"),
            when(expr("rand() < 0.5"), 
                 least(expr("exp(9 + rand() * 1.5)"), lit(500000)))
            .otherwise(least(expr("exp(7 + rand() * 1.2)"), lit(100000)))
            .cast("double").alias("claim_amount"),
            element_at(array([lit(cs) for cs in claim_statuses]), 
                       (rand() * len(claim_statuses) + 1).cast("int")).alias("claim_status"),
            concat(lit("Claim description for "), 
                   element_at(array([lit(ct) for ct in claim_types]), 
                             (rand() * len(claim_types) + 1).cast("int")), 
                   lit(" incident")).alias("description"),
            (rand() < lit(fraud_rate)).cast(BooleanType()).alias("is_fraud"),
            format_string("ADJ%03d", (rand() * num_adjusters + 1).cast("int")).alias("adjuster_id"),
            (rand() * 89 + 1).cast("int").alias("processing_days")
        )
        
        # Append to table
        claims_batch_df.write.mode("append").saveAsTable(f"{catalog}.{schema}.claims")
    
    # Read back the complete claims table
    print("Reading generated claims...")
    claims_df = spark.table(f"{catalog}.{schema}.claims")
    
    claims_count = claims_df.count()
    fraud_count = claims_df.filter(col("is_fraud") == True).count()
    
    print(f"✓ Generated {claims_count:,} claims")
    print(f"✓ Fraudulent claims: {fraud_count:,} ({fraud_count/claims_count*100:.1f}%)")
    print(f"  Note: Fraud rings will be discovered through recursive analysis, not pre-labeled")
    
else:
    # Use pandas for smaller datasets
    print(f"Using pandas-based generation for {num_claims:,} claims...")
    
    def generate_claims_pandas(policyholders_df, n_claims=5000, fraud_rate=0.15):
        """Generate insurance claims with fraud patterns"""
        claims = []
        policyholder_ids = [row.policyholder_id for row in policyholders_df.select("policyholder_id").collect()]
        policyholders_pdf = policyholders_df.toPandas()
        
        claim_types = ['Auto', 'Home', 'Health', 'Property', 'Liability']
        claim_statuses = ['Pending', 'Approved', 'Denied', 'Under Review']
        
        for i in range(n_claims):
            policyholder_id = random.choice(policyholder_ids)
            
            # Determine fraud - policyholders with shared attributes have higher fraud rates
            # This creates discoverable patterns
            ph_data = policyholders_pdf[policyholders_pdf['policyholder_id'] == policyholder_id]
            if len(ph_data) > 0:
                # Check if this policyholder shares address/phone with others (potential fraud cluster)
                shared_address_count = len(policyholders_pdf[policyholders_pdf['address'] == ph_data['address'].values[0]])
                shared_phone_count = len(policyholders_pdf[policyholders_pdf['phone'] == ph_data['phone'].values[0]])
                has_shared_attributes = shared_address_count > 1 or shared_phone_count > 1
                
                if has_shared_attributes:
                    # Higher fraud rate for policyholders with shared attributes
                    is_fraud = np.random.random() < (fraud_rate * 2)  # 2x fraud rate
                else:
                    is_fraud = np.random.random() < fraud_rate
            else:
                is_fraud = np.random.random() < fraud_rate
            
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
                'adjuster_id': f'ADJ{np.random.randint(1, num_adjusters+1):03d}',
                'processing_days': np.random.randint(1, 90)
            })
        
        return pd.DataFrame(claims)
    
    claims_pdf = generate_claims_pandas(policyholders_df, n_claims=num_claims, fraud_rate=fraud_rate)
    claims_df = spark.createDataFrame(claims_pdf)
    claims_df = claims_df.withColumn('claim_date', to_date(col('claim_date'))) \
                         .withColumn('incident_date', to_date(col('incident_date')))
    
    claims_count = len(claims_pdf)
    fraud_count = claims_pdf['is_fraud'].sum()
    
    print(f"✓ Generated {claims_count:,} claims")
    print(f"✓ Fraudulent claims: {fraud_count:,} ({fraud_count/claims_count*100:.1f}%)")
    print(f"  Note: Fraud rings will be discovered through recursive analysis, not pre-labeled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Adjusters
# MAGIC 
# MAGIC **What are Adjusters?**
# MAGIC 
# MAGIC Insurance adjusters are professionals who investigate and process insurance claims. In fraud detection, they play a key role:
# MAGIC 
# MAGIC - **Service Provider Connections**: Claims handled by the same adjuster within a time window can indicate relationships (one of the three relationship types used in fraud detection)
# MAGIC - **Pattern Detection**: If an adjuster handles multiple suspicious claims, this can indicate coordinated fraud or internal fraud
# MAGIC - **Special Investigations Unit (SIU)**: Some adjusters are part of the SIU, which specifically investigates potential fraud cases
# MAGIC 
# MAGIC **Why This Matters for Fraud Detection:**
# MAGIC 
# MAGIC In production systems, service provider connections (including adjusters, repair shops, medical providers, attorneys) are a critical relationship type for discovering fraud networks. The recursive queries use these connections to find related claims and build fraud networks.

# COMMAND ----------

print("Generating adjusters...")

def generate_adjusters(n=50):
    """Generate insurance adjuster data
    
    Adjusters are assigned to claims and can be used to detect fraud patterns:
    - Claims handled by the same adjuster within a time window are related (service provider connection)
    - SIU members are specialized fraud investigators
    - Department indicates the type of claims they handle
    """
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
# MAGIC ## Step 6: Write to Delta Tables

# COMMAND ----------

print("\nWriting data to Delta tables...")

# Drop tables if overwrite mode to ensure clean schema
if overwrite_mode:
    print("Overwrite mode: Dropping existing tables...")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.policyholders")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.adjusters")
    # Claims table already dropped earlier in batch processing
    print("✓ Existing tables dropped")

write_mode = "overwrite" if overwrite_mode else "append"

# Write policyholders with schema merge support
policyholders_df.write.mode(write_mode).option("mergeSchema", "true").saveAsTable(f"{catalog}.{schema}.policyholders")
print(f"✓ Written to {catalog}.{schema}.policyholders")

# Write claims (already handled in batch processing for large datasets)
if not use_batch_processing:
    claims_df.write.mode(write_mode).option("mergeSchema", "true").saveAsTable(f"{catalog}.{schema}.claims")
    print(f"✓ Written to {catalog}.{schema}.claims")
else:
    print(f"✓ Claims already written via batch processing to {catalog}.{schema}.claims")

# Write adjusters with schema merge support
adjusters_df.write.mode(write_mode).option("mergeSchema", "true").saveAsTable(f"{catalog}.{schema}.adjusters")
print(f"✓ Written to {catalog}.{schema}.adjusters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Enriched Views

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
# MAGIC ## Step 8: Dataset Summary

# COMMAND ----------

# Calculate summary statistics
if use_batch_processing:
    total_claims = claims_df.count()
    fraudulent_claims = claims_df.filter(col("is_fraud") == True).count()
    fraud_percentage = (fraudulent_claims / total_claims) * 100 if total_claims > 0 else 0
    total_amount = claims_df.agg({"claim_amount": "sum"}).collect()[0][0] or 0
    fraud_amount = claims_df.filter(col("is_fraud") == True).agg({"claim_amount": "sum"}).collect()[0][0] or 0
    fraud_amount_percentage = (fraud_amount / total_amount) * 100 if total_amount > 0 else 0
else:
    total_claims = claims_pdf.shape[0]
    fraudulent_claims = claims_pdf['is_fraud'].sum()
    fraud_percentage = (fraudulent_claims / total_claims) * 100
    total_amount = claims_pdf['claim_amount'].sum()
    fraud_amount = claims_pdf[claims_pdf['is_fraud']]['claim_amount'].sum()
    fraud_amount_percentage = (fraud_amount / total_amount) * 100 if total_amount > 0 else 0

print("=" * 60)
print("DATASET GENERATION COMPLETE")
print("=" * 60)
print(f"\n📊 Dataset Summary:")
print(f"  • Policyholders: {num_policyholders:,}")
print(f"  • Claims: {total_claims:,}")
print(f"  • Fraudulent Claims: {fraudulent_claims:,} ({fraud_percentage:.1f}%)")
print(f"  • Fraud Rings: Will be discovered through recursive analysis")
print(f"  • Relationships: Computed on-demand in recursive queries (production approach)")
print(f"  • Adjusters: {num_adjusters}")
print(f"\n💰 Financial Summary:")
print(f"  • Total Claim Amount: ${total_amount:,.2f}")
print(f"  • Fraudulent Claim Amount: ${fraud_amount:,.2f}")
print(f"  • Fraud Amount Percentage: {fraud_amount_percentage:.1f}%")
print(f"\n📁 Tables Created:")
print(f"  • {catalog}.{schema}.policyholders")
print(f"  • {catalog}.{schema}.claims")
print(f"  • {catalog}.{schema}.adjusters")
print(f"  • {catalog}.{schema}.claims_enriched (view)")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Preview Data

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
# MAGIC **Proceed to the next notebook:**
# MAGIC 
# MAGIC **02_Recursive_Fraud_Detection.py** - Run recursive queries to detect fraud networks using stored procedures. Relationships will be computed on-demand during recursive traversal (production approach).
# MAGIC 
# MAGIC **Note:** Make sure to update the catalog and schema names in the subsequent notebook to match your configuration above.

