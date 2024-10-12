import pandas as pd
import powerlaw
import matplotlib.pyplot as plt
from scipy.stats import chisquare

expected_ratio = 2
#file_name = 'realcount.csv'
#file_name = 'w100000_10d.csv'
file_name = 'w1000000_10d.csv'
#file_name = 'w59M_5d.csv'



# Load CSV file
df = pd.read_csv(f'./data/all_{file_name}')
df_verified = pd.read_csv(f'./data/verified_{file_name}')

# Extract the frequency column to fit the power-law model
frequency_data = df['frequency']
frequency_data_verified = df_verified['frequency']

# Fit a power-law distribution to the data
fit = powerlaw.Fit(frequency_data, verbose=False)
fit_verified = powerlaw.Fit(frequency_data_verified, verbose=False)

# Get the alpha value (exponent parameter) and xmin (starting point of the power-law tail)
df_alpha = fit.alpha
df_threshold = fit.xmin

alpha_verified = fit_verified.alpha
threshold_verified = fit_verified.xmin

threshold = max(df_threshold, threshold_verified)

print('df_threshold:', df_threshold)
print('threshold_verified:', threshold_verified)
#print('Final Threshold:', threshold)

percentile = (1 - 0.0017195426)
t = df['frequency'].quantile(percentile)
print('Final Threshold:', t)

# Filter df where frequency >= threshold
df_t = df[(df['frequency'] >= t)]

# Initialize an empty list to store the value counts for each column
t_values_list = []
is_attributed_values_list = []

# Loop through each column in df_t and count the occurrences of each value, excluding rows where value is 1
for column in ['app', 'device', 'os', 'channel']:
    value_counts = df_t[column].value_counts().to_dict()  # Convert value counts to a dictionary
    t_values_list.append({column: value_counts})  # Append column name and value counts to the list

    values_counts_isattributed = df_verified[column].value_counts().to_dict()
    is_attributed_values_list.append({column: values_counts_isattributed})


# Perform chi-square tests and identify anomalies
anomalies = []
anomalous_values = []

for i, column in enumerate(['app', 'device', 'os', 'channel']):
    # Extract the value counts for each column from both lists
    t_values = t_values_list[i][column]
    is_attributed_values = is_attributed_values_list[i][column]

    # Align the indices of both dictionaries by filling missing values with zero
    all_keys = set(t_values.keys()).union(set(is_attributed_values.keys()))
    t_frequencies = [t_values.get(key, 0) for key in all_keys]
    is_attributed_frequencies = [is_attributed_values.get(key, 0) for key in all_keys]

    # Filter keys further to include only those where observed is greater than expected
    final_filtered_keys = [
        key for key, t_freq, exp_freq in zip(all_keys, t_frequencies, is_attributed_frequencies)
        if t_freq > exp_freq
    ]
    t_frequencies = [t_values.get(key, 0) for key in final_filtered_keys]
    is_attributed_frequencies = [is_attributed_values.get(key, 0) for key in final_filtered_keys]

    # Normalize frequencies to ensure they have the same sum
    total_t_frequencies = sum(t_frequencies)
    total_is_attributed_frequencies = sum(is_attributed_frequencies)

    if total_t_frequencies == 0 or total_is_attributed_frequencies == 0:
        continue

    t_frequencies = [freq * (total_is_attributed_frequencies / total_t_frequencies) for freq in t_frequencies]

    # Add a small value to expected frequencies to avoid division by zero
    is_attributed_frequencies = [max(freq, 1e-10) for freq in is_attributed_frequencies]

    # Perform the chi-square test
    chi2, p = chisquare(f_obs=t_frequencies, f_exp=is_attributed_frequencies)

    # Identify anomalies based on a significance threshold (e.g., p < 0.05)
    if p < 0.01:
        anomalies.append({
            'column': column,
            'chi2': chi2,
            'p_value': p,
            't_frequencies': t_frequencies,
            'is_attributed_frequencies': is_attributed_frequencies
        })

        # Identify specific values contributing to anomalies
        column_anomalous_values = []
        for j, key in enumerate(final_filtered_keys):
            # Calculate the absolute difference between observed and expected frequencies
            diff = (t_frequencies[j] - is_attributed_frequencies[j])
            if (diff > expected_ratio * is_attributed_frequencies[j]) & (diff > 3):  # Consider values with a significant difference
                column_anomalous_values.append({ 
                    'value': key,
                    'observed': t_frequencies[j],
                    'expected': is_attributed_frequencies[j],
                    'difference': diff
                })

        anomalous_values.append({
            'column': column,
            'anomalous_values': column_anomalous_values
        })

## Print the anomalies
#for anomaly in anomalies:
#    print(f"Anomaly detected in column: {anomaly['column']}")
#    print(f"Chi-square statistic: {anomaly['chi2']}, p-value: {anomaly['p_value']}")

# Print the values contributing to anomalies
for column_values in anomalous_values:
    print(f"\nAnomalous values in column: {column_values['column']}")
    for value_info in column_values['anomalous_values']:
        print(f"Value: {value_info['value']}, Observed: {value_info['observed']}, "
              f"Expected: {value_info['expected']}, Difference: {value_info['difference']}")
        
# Write anomalous values to a CSV file
anomaly_file_path = f'anomalous_values_{file_name}'
with open(anomaly_file_path, 'w') as file:
    file.write('column,value,observed,expected,difference\n')
    for column_values in anomalous_values:
        for value_info in column_values['anomalous_values']:
            file.write(f"{column_values['column']},{value_info['value']},"
                       f"{value_info['observed']},{value_info['expected']},{value_info['difference']}\n")


# Create an empty list to store the results
anomaly_thresholds = []

# Iterate over each column-value pair in the anomalous list
for column_values in anomalous_values:
    column = column_values['column']
    for value_info in column_values['anomalous_values']:
        value = value_info['value']

        # Filter df_t to get the frequency data for the specific column-value pair
        frequency_data = df[df[column] == value]['frequency']

        # If frequency_data is empty, skip this iteration
        if frequency_data.empty:
            continue

        # Fit a power-law model to the frequency data
        fit = powerlaw.Fit(frequency_data, verbose=False)

        # Get the alpha value and xmin (starting point of the power-law tail)
        alpha = fit.alpha
        threshold = fit.xmin

        # Store the column, value, frequency details, and threshold to the list
        anomaly_thresholds.append({
            'column': column,
            'value': value,
            'threshold': threshold
        })

# Convert the list to a DataFrame
anomaly_thresholds_df = pd.DataFrame(anomaly_thresholds)

# Display the DataFrame
print(anomaly_thresholds_df)

# Save the DataFrame to a CSV file if needed
anomaly_thresholds_df.to_csv(f'anomalous_thresholds_{file_name}', index=False)


print('Global threshold: ', t)
