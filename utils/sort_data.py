import pandas as pd

df = pd.read_csv('data/train_sample.csv')

df['click_time'] = pd.to_datetime(df['click_time'])

df_sorted = df.sort_values(by='click_time')

df_sorted.to_csv('sorted_file.csv', index=False)
