import pandas as pd

df = pd.read_csv('./data/verified_realcount.csv')

# Filter the rows with frequency equal to 16
print(len(df))

print(2/116310)

value = 1.7195426016679562e-05
percentage = value * 100
print(f'{percentage:.10f}%')
