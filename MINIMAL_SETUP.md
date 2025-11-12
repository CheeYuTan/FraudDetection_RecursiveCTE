# Minimal Setup Guide

This project is already minimal! All unnecessary files have been removed.

## Required Files

```
notebooks/
├── 01_Dataset_Generation.py          # ⭐ Generate data in Databricks
├── 02_Recursive_Fraud_Detection.py    # ⭐ Fraud detection queries
└── 03_Fraud_Analysis_Visualization.py # ⭐ Analysis and visualization
```

That's it! Just 3 notebooks.

## Project Structure

```
.
├── README.md                          # Complete documentation
├── QUICKSTART.md                      # Quick start guide
├── MINIMAL_SETUP.md                   # This file
├── DATABRICKS_NOTES.md                # Implementation notes
├── PROJECT_SUMMARY.md                  # Project overview
├── notebooks/                         # Databricks notebooks (3 files)
└── sql_scripts/                       # Standalone SQL (optional reference)
```

## Quick Start

1. Clone the repository in Databricks:
   - Go to **Workspace** → Click dropdown next to username → **Git** → **Clone Repository**
   - URL: `https://github.com/CheeYuTan/FraudDetection_RecursiveCTE.git`
2. Open `notebooks/01_Dataset_Generation.py` and set widgets (catalog, schema, etc.)
3. Run it
4. Open and run `notebooks/02_Recursive_Fraud_Detection.py` (set same catalog/schema)
5. Open and run `notebooks/03_Fraud_Analysis_Visualization.py` (set same catalog/schema)

Done! 🎉

## What Was Removed

The following files were removed to keep the project minimal:
- ❌ `generate_fraud_dataset.py` - Local Python script (not needed)
- ❌ `01_Data_Ingestion.py` - CSV loading notebook (not needed)
- ❌ `data/` folder - CSV files (not needed)
- ❌ `requirements.txt` - Python dependencies (not needed)

All data generation now happens directly in Databricks using `01_Dataset_Generation.py`!
