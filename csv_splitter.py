import pandas as pd

# Load the CSV file into a DataFrame
csv_file_path = './data/24h_processed_isattributed.csv'  # Update this with the path to your CSV file
df = pd.read_csv(csv_file_path)

# Filter rows based on the 'is_attributed' column
df_attributed = df[df['is_attributed'] == 1].drop(columns=['is_attributed'])
df_not_attributed = df[df['is_attributed'] == 0].drop(columns=['is_attributed'])

# Save the filtered dataframes to separate CSV files
df_not_attributed.to_csv('unique_keys_output_realcount.csv', index=False)
df_attributed.to_csv('verified_users_output_realcount.csv', index=False)

print("CSV files have been split and saved successfully.")