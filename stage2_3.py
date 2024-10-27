import pandas as pd
import powerlaw
import matplotlib.pyplot as plt
from scipy.stats import chisquare
import os as osos
import csv
import redis
import time

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

width = 50000
depth = 10

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


input_dir = 'input'
keys_dir = './identifiers/'

# Directory containing the 2-hour chunks
output_dir = 'stage23_rules'
count_output_dir = 'stage23_count'

osos.makedirs(output_dir, exist_ok=True)
osos.makedirs(count_output_dir, exist_ok=True)


times = []


# Stage 2: Load Data and Query Count-Min Sketch
start_stage = time.time()
# Iterate over all CSV files in the directory
for file_name in osos.listdir(input_dir):
    if file_name.endswith('.csv'):
        file_path = osos.path.join(input_dir, file_name)
        start_csv = time.time()
        
        # Create Count-Min Sketch if not exists
        CMS_KEY = f'cms_1kk:{file_name}'


        # Load data from CSV files
        print("Loading data from CSV files...")

        df_all_keys = pd.read_csv(f'{keys_dir}users_{file_name}')
        df_verified_keys = pd.read_csv(f'{keys_dir}verified_users_{file_name}')

        print("Data loaded successfully!")


        verified_users_keys = df_verified_keys['verified_user_key'].values

        verified_users_set = set(verified_users_keys)
        df_all_keys = df_all_keys[~df_all_keys['unique_key'].isin(verified_users_set)]

        filtered_unique_keys = df_all_keys['unique_key'].values

        BATCH_SIZE = 100000

        # Use a Redis pipeline to query the Count-Min Sketch for unique keys
        print("Preparing to query all keys...")
        all_keys = []
        batch_keys = []

        with r.pipeline() as pipe:
            for idx, key in enumerate(filtered_unique_keys):
                pipe.cms().query(CMS_KEY, key)
                batch_keys.append(key)

                # Execute the pipeline in batches
                if (idx + 1) % BATCH_SIZE == 0:
                    all_keys.extend(pipe.execute())
                    pipe.reset()
                    batch_keys = []
                    print(f"Processed {idx + 1} unique keys")


            # Execute any remaining keys in the pipeline
            if batch_keys:
                all_keys.extend(pipe.execute())

        print("All keys queried successfully!")

        # Decode the unique keys and counts into the desired format
        formatted_output_unique = []
        print("Formatting unique keys...")
        for key, count in zip(filtered_unique_keys, all_keys):
            ip, app, device, os, channel = key.split('-')
            identifier = key
            frequency = count[0] if count else 0
            formatted_output_unique.append(f"{identifier},{ip},{app},{device},{os},{channel},{frequency}")
        print("Unique keys formatted successfully!")

        # Use a Redis pipeline to query the Count-Min Sketch for verified users keys
        print("Preparing to query verified users keys...")
        verified_users_counts = []
        batch_keys = []
        with r.pipeline() as pipe:
            for idx, key in enumerate(verified_users_keys):
                pipe.cms().query(CMS_KEY, key)
                batch_keys.append(key)

                # Execute the pipeline in batches
                if (idx + 1) % BATCH_SIZE == 0:
                    verified_users_counts.extend(pipe.execute())
                    pipe.reset()
                    batch_keys = []
                    print(f"Processed {idx + 1} verified users keys")
            if batch_keys:
                verified_users_counts.extend(pipe.execute())

        print("Verified users keys queried successfully!")

        # Decode the verified users keys and counts into the desired format
        formatted_output_verified = []
        print("Formatting verified users keys...")
        for key, count in zip(verified_users_keys, verified_users_counts):
            ip, app, device, os, channel = key.split('-')
            identifier = key
            frequency = count[0] if count else 0
            formatted_output_verified.append(f"{identifier},{ip},{app},{device},{os},{channel},{frequency}")
        print("Verified users keys formatted successfully!")


        # Convert formatted_output_unique to a pandas DataFrame
        print("Converting unique_keys_output to a pandas DataFrame...")
        unique_columns = ["identifier", "ip", "app", "device", "os", "channel", "frequency"]
        unique_data = [line.split(',') for line in formatted_output_unique]
        df = pd.DataFrame(unique_data, columns=unique_columns)

        # Convert frequency column to numeric
        df['frequency'] = pd.to_numeric(df['frequency'], errors='coerce')

        print("unique_keys_output converted successfully!")

        

        # Convert formatted_output_verified to a pandas DataFrame
        print("Converting verified_users_output to a pandas DataFrame...")
        verified_data = [line.split(',') for line in formatted_output_verified]
        df_verified = pd.DataFrame(verified_data, columns=unique_columns)

        # Convert frequency column to numeric
        df_verified['frequency'] = pd.to_numeric(df_verified['frequency'], errors='coerce')

        print("verified_users_output converted successfully!")

        # Save the unique keys and verified users keys to CSV files
        unique_output_file = osos.path.join(count_output_dir, f'all_count_{file_name}')
        verified_output_file = osos.path.join(count_output_dir, f'verified_count_{file_name}')

        df.to_csv(unique_output_file, index=False)
        df_verified.to_csv(verified_output_file, index=False)
        print(df.head())
        print(df_verified.head())


        ############################################################################################################
        # Stage 3: Identify Anomalies in the Data

        expected_ratio = 4
        significance_threshold = 0.01
        


        # Extract the frequency column to fit the power-law model
        frequency_data = df['frequency']
        frequency_data_verified = df_verified['frequency']

        # Fit a power-law distribution to the data
        fit = powerlaw.Fit(frequency_data, verbose=False)
        fit_verified = powerlaw.Fit(frequency_data_verified, verbose=False)

        # Get the alpha value (exponent parameter) and xmin (starting point of the power-law tail)
        df_threshold = fit.xmin

        t = fit_verified.xmin

        print('df_threshold:', df_threshold)
        print('threshold_verified:', t)

      

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
            if p < significance_threshold:
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


        # Print the values contributing to anomalies
        for column_values in anomalous_values:
            print(f"\nAnomalous values in column: {column_values['column']}")
            for value_info in column_values['anomalous_values']:
                print(f"Value: {value_info['value']}, Observed: {value_info['observed']}, "
                      f"Expected: {value_info['expected']}, Difference: {value_info['difference']}")

        # Write anomalous values to a CSV file
        anomaly_file_path = osos.path.join(output_dir, f'raw_values_{file_name}')
        #anomaly_file_path = osos.path.join(output_dir_realcount, f'anomalous_values_{file_name}')
        #anomaly_file_path = f'anomalous_values_{file_name}'
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
        output_file = osos.path.join(output_dir, f'rules_{file_name}') #count
        #output_file = osos.path.join(output_dir_realcount, f'rules_realcount_{file_name}') #realcount
        anomaly_thresholds_df.to_csv(f'{output_file}', index=False)

        # Save the global threshold to a CSV file
        file_exists = osos.path.isfile('thresholds.csv')
        #file_exists = osos.path.isfile('thresholds_realcount.csv')


        #with open('thresholds.csv', 'a', newline='') as file:
        with open('thresholds.csv', 'a', newline='') as file:
            writer = csv.writer(file)
            # Write header if the file does not exist
            if not file_exists:
                writer.writerow(['file_name', 'df_threshold', 'threshold_verified', 'percentile_threshold'])

            # Write the data row
            writer.writerow([file_name, df_threshold, t, t])


        print('Global threshold: ', t)
        end_stage = time.time()
        print('Time taken for file:', file_name, 'is', end_stage - start_stage)
        times.append((file_name, end_stage - start_stage))

time_end = time.time()
print(times[:1000])
