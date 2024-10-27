import pandas as pd
import os as osos

realcount = False


# Directory containing the 2-hour chunks
file_name_dir = './input/'
keys_dir = './identifiers/'

if realcount:
    rules_dir = './data_realcount/stage3_rules/'
    count_dir = './data_realcount/stage2_counts/'
    output_dir = './data_realcount/stage4_filtered_users'
    output_dir_CN = './data_realcount/stage4_filtered_users_compareNext'
    t_df = pd.read_csv('thresholds_realcount.csv')
else:
    rules_dir = './stage23_rules/'
    count_dir = './stage23_count/'
    output_dir = 'stage4_filtered_users'
    output_dir_CN = 'stage4_filtered_users_compareNext'
    t_df = pd.read_csv('thresholds.csv')


t_df.columns = t_df.columns.str.strip()

osos.makedirs(output_dir, exist_ok=True) #sketch
osos.makedirs(output_dir_CN, exist_ok=True) #compareNext

# Create dataframe for previous time period
previous_thresholds_df = pd.DataFrame()

# Iterate over all CSV files in the directory
for file_name in osos.listdir(file_name_dir):
    if file_name.endswith('.csv'):
        file_path = osos.path.join(file_name_dir, file_name)
        
        # Extract the row where file_name matches and get the value for df_threshold
        matching_row = t_df.loc[t_df['file_name'] == file_name]
        if not matching_row.empty:
            df_threshold_value = matching_row['df_threshold'].values[0]
            print(f"Extracted df_threshold value: {df_threshold_value}")
        else:
            print(f"No matching row found for file_name: {file_name}")

        global_threshold = df_threshold_value
        print(f"Global threshold: {global_threshold}")
        
        thresholds_df = pd.read_csv(f'{rules_dir}rules_{file_name}')
        df = pd.read_csv(f'{count_dir}all_count_{file_name}') #real_count
        
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
        
        # Concatenate all the filtered rows into a single DataFrame
        result_df = pd.concat(above_threshold_rows)
        result_df = result_df.drop_duplicates()
        result_df_len = len(result_df)
        
        # Display the resulting DataFrame
        print(result_df)

        # Save the resulting DataFrame to a CSV file
        output_file = osos.path.join(output_dir, f'rules_filter_{file_name}') #realcount
        result_df.to_csv(f'{output_file}', index=False)

        # Add above frequency threshold to the result list
        above_threshold_rows.append(df_topT)
        result_df = pd.concat(above_threshold_rows)
        result_df = result_df.drop_duplicates()

        output_file = osos.path.join(output_dir, f'rules_freq_filter_{file_name}') #realcount
        result_df.to_csv(f'{output_file}', index=False)

        df_len = len(df)

        print('#Users above global threshold: ', df_topT_len)
        print('#Users above rule-based thresholds: ', result_df_len)
        print('Total #Users above thresholds: ', df_topT_len + result_df_len)
        print('Total #Users: ', df_len)
        print('Percentage of users above thresholds: ', (df_topT_len + result_df_len) / df_len * 100, '%')

        
            
        if not previous_thresholds_df.empty:
                
                # Initialize a list to store rows from df that exceed the threshold
                above_threshold_rows = []

                # Iterate over each row in the thresholds DataFrame
                for _, row in previous_thresholds_df.iterrows():
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

                # Concatenate all the filtered rows into a single DataFrame
                result_df = pd.concat(above_threshold_rows)
                result_df = result_df.drop_duplicates()
                result_df_len = len(result_df)

                # Display the resulting DataFrame
                print(result_df)

                # Save the resulting DataFrame to a CSV file
                output_file = osos.path.join(output_dir_CN, f'rules_filter_CN_{file_name}') #realcount
                result_df.to_csv(f'{output_file}', index=False)

                # Add above frequency threshold to the result list
                above_threshold_rows.append(df_topT)
                result_df = pd.concat(above_threshold_rows)
                result_df = result_df.drop_duplicates()

                output_file = osos.path.join(output_dir_CN, f'rules_freq_filter_CN_{file_name}') #realcount
                result_df.to_csv(f'{output_file}', index=False)

                df_len = len(df)

                print('#Users above global threshold: ', df_topT_len)
                print('#Users above rule-based thresholds: ', result_df_len)
                print('Total #Users above thresholds: ', df_topT_len + result_df_len)
                print('Total #Users: ', df_len)
                print('Percentage of users above thresholds: ', (df_topT_len + result_df_len) / df_len * 100, '%')


                
        previous_thresholds_df = thresholds_df