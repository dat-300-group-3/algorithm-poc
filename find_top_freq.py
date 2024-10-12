import pandas as pd

# Load the CSV file
df = pd.read_csv('./data/all_realcount.csv')

df1 = df[df['frequency'] >= 33]
print(df1)

len_df = len(df)
t = int(len_df * 0.000017195426)

top_freq = df.sort_values(by='frequency', ascending=False).head(t)
print(top_freq)
