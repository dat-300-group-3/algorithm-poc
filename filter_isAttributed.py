import dask.dataframe as dd

# Path to the input CSV file
input_csv_path = './data/train.csv'

# Path for the output filtered CSV
output_csv_path = './data/filtered_is_attributed.csv'

# Explicitly specify the dtype for 'attributed_time' to be 'object' (string)
dtype_spec = {
    'attributed_time': 'object'  # Treat 'attributed_time' as string to avoid conversion issues
}

# Read the CSV file with Dask and specify the dtype
df = dd.read_csv(input_csv_path, dtype=dtype_spec)

# Filter rows where 'is_attributed' is 1
filtered_df = df[df['is_attributed'] == 1]

# Save the filtered rows to a new CSV file
filtered_df.to_csv(output_csv_path, single_file=True, index=False)

print(f"Filtered rows saved to {output_csv_path}")


