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
spark.sql(f"USE {catalog}.{schema}")

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
    
    from pyspark.sql.functions import udf, rand, lit, concat, format_string, when, expr, least, col
    from pyspark.sql.types import StringType, BooleanType, DoubleType
    
    # Get policyholder IDs and attributes for relationship generation
    policyholder_ids_df = policyholders_df.select("policyholder_id", "address", "phone", "city", "state")
    policyholder_ids_list = [row.policyholder_id for row in policyholder_ids_df.collect()]
    
    # Create some policyholders with shared attributes (to create discoverable clusters)
    # This simulates real-world scenarios where fraud rings share addresses/phones
    policyholders_pdf = policyholders_df.toPandas()
    n_shared_groups = max(1, min(100, int(num_policyholders * 0.1)))  # ~10% of policyholders in shared groups
    
    for group_id in range(n_shared_groups):
        group_size = np.random.randint(3, 8)
        group_members = random.sample(range(len(policyholders_pdf)), min(group_size, len(policyholders_pdf)))
        # Assign shared address and phone to group members
        shared_address = f'{np.random.randint(100, 9999)} Shared St'
        shared_phone = f'{np.random.randint(100, 999)}-{np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}'
        for idx in group_members:
            policyholders_pdf.iloc[idx, policyholders_pdf.columns.get_loc('address')] = shared_address
            policyholders_pdf.iloc[idx, policyholders_pdf.columns.get_loc('phone')] = shared_phone
    
    # Update policyholders_df with shared attributes
    policyholders_df = spark.createDataFrame(policyholders_pdf)
    policyholders_df = policyholders_df.withColumn('policy_start_date', to_date(col('policy_start_date')))
    
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
            # Fraud probability - some policyholders with shared attributes have higher fraud rates
            # This creates discoverable fraud patterns
            return random.random() < fraud_rate
        
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
            format_string("ADJ%03d", (rand() * num_adjusters + 1).cast("int")).alias("adjuster_id"),
            (rand() * 89 + 1).cast("int").alias("processing_days")
        )
        
        all_claims_dfs.append(claims_batch_df)
    
    # Union all batches
    print("Combining batches...")
    claims_df = all_claims_dfs[0]
    for df in all_claims_dfs[1:]:
        claims_df = claims_df.union(df)
    
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
# MAGIC ## Step 5: Generate Claim Relationships

# COMMAND ----------

print("Generating claim relationships...")

if use_batch_processing:
    print("Using Spark-based relationship generation...")
    from pyspark.sql.functions import col, rand, lit, datediff, abs as spark_abs
    
    # Join claims with policyholders to get attributes
    claims_with_ph = claims_df.join(
        policyholders_df.select("policyholder_id", "address", "phone", "city", "state"),
        "policyholder_id"
    )
    
    all_relationships = []
    
    # 1. Policyholder connections: Claims from related policyholders (same address, phone, etc.)
    print("  Generating policyholder connections...")
    policyholder_conn_rels = claims_with_ph.alias("c1").join(
        claims_with_ph.alias("c2"),
        (
            # Same address or same phone number
            ((col("c1.address") == col("c2.address")) & col("c1.address").isNotNull()) |
            ((col("c1.phone") == col("c2.phone")) & col("c1.phone").isNotNull())
        ) &
        (col("c1.policyholder_id") != col("c2.policyholder_id")) &
        (col("c1.claim_id") < col("c2.claim_id"))
    ).select(
        col("c1.claim_id").alias("claim_id_1"),
        col("c2.claim_id").alias("claim_id_2"),
        lit("policyholder_connection").alias("relationship_type"),
        (rand() * 0.2 + 0.7).alias("strength")  # 0.7 to 0.9
    )
    all_relationships.append(policyholder_conn_rels)
    
    # 2. Temporal patterns: Claims filed within short time windows
    print("  Generating temporal patterns...")
    temporal_rels = claims_df.alias("c1").join(
        claims_df.alias("c2"),
        (col("c1.claim_type") == col("c2.claim_type")) &
        (col("c1.claim_id") < col("c2.claim_id")) &
        (spark_abs(datediff(col("c1.claim_date"), col("c2.claim_date"))) < 30) &  # Within 30 days
        (spark_abs(col("c1.claim_amount") - col("c2.claim_amount")) < col("c1.claim_amount") * 0.3)  # Similar amounts
    ).select(
        col("c1.claim_id").alias("claim_id_1"),
        col("c2.claim_id").alias("claim_id_2"),
        lit("temporal_pattern").alias("relationship_type"),
        (rand() * 0.3 + 0.5).alias("strength")  # 0.5 to 0.8
    )
    all_relationships.append(temporal_rels)
    
    # 3. Service provider connections: Same adjuster, repair shop, medical provider, etc.
    print("  Generating service provider connections...")
    service_provider_rels = claims_df.alias("c1").join(
        claims_df.alias("c2"),
        (col("c1.adjuster_id") == col("c2.adjuster_id")) &
        (col("c1.claim_id") < col("c2.claim_id")) &
        (spark_abs(datediff(col("c1.claim_date"), col("c2.claim_date"))) < 90)  # Within 90 days
    ).select(
        col("c1.claim_id").alias("claim_id_1"),
        col("c2.claim_id").alias("claim_id_2"),
        lit("service_provider_connection").alias("relationship_type"),
        (rand() * 0.2 + 0.6).alias("strength")  # 0.6 to 0.8
    )
    all_relationships.append(service_provider_rels)
    
    # Union all relationship types
    print("  Combining all relationship types...")
    relationships_df = all_relationships[0]
    for rel_df in all_relationships[1:]:
        relationships_df = relationships_df.union(rel_df)
    
    # Remove duplicates
    relationships_df = relationships_df.dropDuplicates(["claim_id_1", "claim_id_2"])
    
    # Limit relationships for very large datasets
    if num_claims > 1_000_000:
        max_relationships = min(10_000_000, int(num_claims * 0.1))
        relationships_df = relationships_df.limit(max_relationships)
        print(f"  Limiting relationships to {max_relationships:,} for performance")
    
    relationships_count = relationships_df.count()
    print(f"✓ Generated {relationships_count:,} relationships")
    print(f"  Relationship types: policyholder_connection, temporal_pattern, service_provider_connection")
    
else:
    # Use pandas for smaller datasets
    def generate_relationships_pandas(claims_df, policyholders_df):
        """Generate relationships between claims using 3 types: policyholder connections, temporal patterns, service provider connections"""
        relationships = []
        claims_pdf = claims_df.toPandas()
        policyholders_pdf = policyholders_df.toPandas()
        
        # Merge to get policyholder attributes
        claims_with_ph = claims_pdf.merge(
            policyholders_pdf[['policyholder_id', 'address', 'phone', 'city', 'state']],
            on='policyholder_id',
            how='left'
        )
        
        # 1. Policyholder connections: Claims from related policyholders (same address, phone, etc.)
        for address in claims_with_ph['address'].unique():
            if pd.isna(address):
                continue
            address_claims = claims_with_ph[claims_with_ph['address'] == address]
            if len(address_claims) > 1:
                claim_ids = address_claims['claim_id'].tolist()
                for i, claim1 in enumerate(claim_ids):
                    for claim2 in claim_ids[i+1:]:
                        # Only connect if different policyholders
                        ph1 = address_claims[address_claims['claim_id'] == claim1]['policyholder_id'].values[0]
                        ph2 = address_claims[address_claims['claim_id'] == claim2]['policyholder_id'].values[0]
                        if ph1 != ph2 and np.random.random() < 0.5:  # 50% chance
                            relationships.append({
                                'claim_id_1': claim1,
                                'claim_id_2': claim2,
                                'relationship_type': 'policyholder_connection',
                                'strength': np.random.uniform(0.7, 0.9)
                            })
        
        for phone in claims_with_ph['phone'].unique():
            if pd.isna(phone):
                continue
            phone_claims = claims_with_ph[claims_with_ph['phone'] == phone]
            if len(phone_claims) > 1:
                claim_ids = phone_claims['claim_id'].tolist()
                for i, claim1 in enumerate(claim_ids):
                    for claim2 in claim_ids[i+1:]:
                        ph1 = phone_claims[phone_claims['claim_id'] == claim1]['policyholder_id'].values[0]
                        ph2 = phone_claims[phone_claims['claim_id'] == claim2]['policyholder_id'].values[0]
                        if ph1 != ph2 and np.random.random() < 0.5:  # 50% chance
                            relationships.append({
                                'claim_id_1': claim1,
                                'claim_id_2': claim2,
                                'relationship_type': 'policyholder_connection',
                                'strength': np.random.uniform(0.7, 0.9)
                            })
        
        # 2. Temporal patterns: Claims filed within short time windows
        for _, claim1 in claims_pdf.iterrows():
            claim_date1 = pd.to_datetime(claim1['claim_date'])
            similar = claims_pdf[
                (claims_pdf['claim_type'] == claim1['claim_type']) &
                (claims_pdf['claim_id'] != claim1['claim_id']) &
                (abs((pd.to_datetime(claims_pdf['claim_date']) - claim_date1).dt.days) < 30) &
                (abs(claims_pdf['claim_amount'] - claim1['claim_amount']) < claim1['claim_amount'] * 0.3)
            ]
            for _, claim2 in similar.head(2).iterrows():
                if claim1['claim_id'] < claim2['claim_id'] and np.random.random() < 0.3:  # 30% chance
                    relationships.append({
                        'claim_id_1': claim1['claim_id'],
                        'claim_id_2': claim2['claim_id'],
                        'relationship_type': 'temporal_pattern',
                        'strength': np.random.uniform(0.5, 0.8)
                    })
        
        # 3. Service provider connections: Same adjuster, repair shop, medical provider, etc.
        for adjuster_id in claims_pdf['adjuster_id'].unique():
            adjuster_claims = claims_pdf[claims_pdf['adjuster_id'] == adjuster_id]
            for i, claim1 in adjuster_claims.iterrows():
                for j, claim2 in adjuster_claims.iterrows():
                    if i < j:  # Avoid duplicates
                        claim_date1 = pd.to_datetime(claim1['claim_date'])
                        claim_date2 = pd.to_datetime(claim2['claim_date'])
                        if abs((claim_date2 - claim_date1).days) < 90:  # Within 90 days
                            if np.random.random() < 0.4:  # 40% chance
                                relationships.append({
                                    'claim_id_1': claim1['claim_id'],
                                    'claim_id_2': claim2['claim_id'],
                                    'relationship_type': 'service_provider_connection',
                                    'strength': np.random.uniform(0.6, 0.8)
                                })
        
        # Remove duplicates
        relationships_df = pd.DataFrame(relationships)
        if len(relationships_df) > 0:
            relationships_df = relationships_df.drop_duplicates(subset=['claim_id_1', 'claim_id_2'])
        
        return relationships_df if len(relationships_df) > 0 else pd.DataFrame(columns=['claim_id_1', 'claim_id_2', 'relationship_type', 'strength'])
    
    relationships_pdf = generate_relationships_pandas(claims_df, policyholders_df)
    relationships_df = spark.createDataFrame(relationships_pdf)
    relationships_count = len(relationships_pdf) if len(relationships_pdf) > 0 else 0
    print(f"✓ Generated {relationships_count:,} relationships")
    print(f"  Relationship types: policyholder_connection, temporal_pattern, service_provider_connection")

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
print(f"  • Fraud Rings: Will be discovered through recursive analysis")
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

