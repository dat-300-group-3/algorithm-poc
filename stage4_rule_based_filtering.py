import pandas as pd


# Load the thresholds CSV into a DataFrame

#file_name = 'realcount.csv'
#file_name = 'w100000_10d.csv'
file_name = 'w1000000_10d.csv'
#file_name = 'w59M_5d.csv'


#global_threshold = 86 # WE NEED TO LOOK AT THIS ONE <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Has to be higher than 86, since 86 is the threshold for the rule-based filtering
global_threshold = 176 #df_threshold from powerlaw fit

thresholds_df = pd.read_csv(f'anomalous_thresholds_{file_name}')
df = pd.read_csv(f'./data/all_{file_name}')

# Initialize a list to store rows from df that exceed the threshold
above_threshold_rows = []

# Iterate over each row in the thresholds DataFrame
for _, row in thresholds_df.iterrows():
    column = row['column']
    value = row['value']
    threshold = row['threshold']

    # Skip if the threshold is NaN (missing value)
    if pd.isna(threshold):
        continue

    # Filter df to get rows that match the column-value pair and have frequency above the threshold
    filtered_rows = df[(df[column] == value) & (df['frequency'] > threshold)]

    # Append the rows to the result list
    above_threshold_rows.append(filtered_rows)

# Append the largest frequency values to the result list
df_topT = df[df['frequency'] > global_threshold]
df_topT_len = len(df_topT)
print(df_topT)
# Append the rows to the result list
#above_threshold_rows.append(df_topT)

# Concatenate all the filtered rows into a single DataFrame
result_df = pd.concat(above_threshold_rows)


result_df = result_df.drop_duplicates()
result_df_len = len(result_df)# - df_topT_len
# Display the resulting DataFrame
print(result_df)

# Save the resulting DataFrame to a CSV file if needed
result_df.to_csv(f'above_rulebased_threshold_{file_name}', index=False)

df_len = len(df)

print('#Users above global threshold: ', df_topT_len)
print('#Users above rule-based thresholds: ', result_df_len)
print('Total #Users above thresholds: ', df_topT_len + result_df_len)
print('Total #Users: ', df_len)
print('Percentage of users above thresholds: ', (df_topT_len + result_df_len) / df_len * 100, '%')
