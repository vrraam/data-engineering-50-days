import pandas as pd

df = pd.read_csv("/Users/raam/Downloads/Sample - Superstore.csv", encoding='ISO-8859-1')

print("🔍 DATASET OVERVIEW")
print("=" * 50)
print(f"Dataset Shape: {df.shape[0]} rows, {df.shape[1]} columns")

print("\n📋 COLUMN NAMES:")
print("=" * 30)
for i, col in enumerate(df.columns, 1):
    print(f"{i:2d}. {col}")

print("\n📊 DATA TYPES:")
print("=" * 30)
print(df.dtypes)

print("\n🔢 BASIC STATISTICS:")
print("=" * 30)
print(df.describe())

print("\n👀 FIRST 5 ROWS:")
print("=" * 30)
print(df.head())

print("\n❓ MISSING VALUES:")
print("=" * 30)
missing = df.isnull().sum()
print(missing[missing > 0])

print("\n🏪 SAMPLE BUSINESS INFO:")
print("=" * 30)
print(f"Date Range: {df['Order Date'].min()} to {df['Order Date'].max()}")
print(f"Unique Customers: {df['Customer ID'].nunique():,}")
print(f"Unique Products: {df['Product ID'].nunique():,}")
print(f"Total Sales: ${df['Sales'].sum():,.2f}")
print(f"Total Profit: ${df['Profit'].sum():,.2f}")

#To check column list

import pandas as pd

df = pd.read_csv("/Users/raam/Downloads/Sample - Superstore.csv", encoding='ISO-8859-1')
print(df.columns.tolist())

#Clean the data

import pandas as pd

# Read the original CSV with the correct encoding
df = pd.read_csv("/Users/raam/Downloads/Sample - Superstore.csv", encoding='ISO-8859-1')

# Drop the extra 'Row ID' column
df = df.drop(columns=['Row ID'])

# Optional: Rename columns to match PostgreSQL column names (e.g., lowercase with underscores)
df.columns = [col.lower().replace(" ", "_").replace("-", "_") for col in df.columns]

# Save the cleaned version
df.to_csv("/Users/raam/Downloads/superstore_clean.csv", index=False)
