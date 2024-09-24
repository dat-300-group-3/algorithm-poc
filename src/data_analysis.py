

import pandas as pd
import matplotlib.pyplot as plt
import os

# Path to the large CSV file
csv_file = './data/original/test.csv'

# Directory to save the histogram plots
output_dir = 'histograms/'
os.makedirs(output_dir, exist_ok=True)

# Read the entire CSV file
df = pd.read_csv(csv_file)

# List of columns to generate histograms for (excluding non-numeric or timestamp columns)
columns_to_plot = ['ip', 'app', 'device', 'os', 'channel']

# Loop through each column and generate a histogram
for column in columns_to_plot:
    plt.figure(figsize=(10, 6))
    plt.hist(df[column].dropna(), bins=50, color='blue', edgecolor='black')
    plt.title(f'Histogram of {column}')
    plt.xlabel(column)
    plt.ylabel('Frequency')
    
    # Save the plot as a PNG file
    plt.savefig(os.path.join(output_dir, f'{column}_histogram.png'))
    plt.close()

print("Histograms saved to", output_dir)
