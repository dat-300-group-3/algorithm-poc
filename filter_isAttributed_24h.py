import dask.dataframe as dd
from datetime import timedelta

# Path to the input CSV file
input_csv_path = './data/12h_no_isAttributed.csv'

# Path for the output filtered CSV
output_csv_path = './data/12h_device_not_1_2.csv'

# Explicitly specify the dtype for 'attributed_time' and 'click_time' columns
dtype_spec = {
    'attributed_time': 'object',  # Treat 'attributed_time' as string
    'click_time': 'object'  # Initially treat 'click_time' as string before converting
}

# Read the CSV file with Dask and specify the dtype
df = dd.read_csv(input_csv_path, dtype=dtype_spec)

# Convert the 'click_time' column to datetime
df['click_time'] = dd.to_datetime(df['click_time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

# Get the minimum click_time to define the 24-hour window
min_click_time = df['click_time'].min().compute()  # Compute the minimum click time

# Define the 24-hour window
max_click_time = min_click_time + timedelta(hours=12)

# Filter rows where 'device' is 1 and 'click_time' falls within the first 24 hours
#filtered_df = df[(df['device'] == 2) & (df['click_time'] >= min_click_time) & (df['click_time'] <= max_click_time)]
filtered_df = df[(~df['device'].isin([1, 2])) & (df['click_time'] >= min_click_time) & (df['click_time'] <= max_click_time)]

# Save the filtered rows to a new CSV file
filtered_df.to_csv(output_csv_path, single_file=True, index=False)

print(f"Filtered rows saved to {output_csv_path}")
