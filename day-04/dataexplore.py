import pandas as pd

file_path = '/Users/raam/Desktop/50 Days Learning/Day-04/SampleSuperstore.csv'

# Load the CSV
df = pd.read_csv(file_path)

# Show first 5 rows
print(df.head())
print("Columns:", df.columns.tolist())
print(df.dtypes)
