import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file into a DataFrame
# Load the CSV file into a Dask DataFrame
#file_name = 'realcount.csv'
#file_name = 'w100000_10d.csv'
file_name = 'w1000000_10d.csv'
#file_name = 'w59M_5d.csv'

df = pd.read_csv(f'./data/all_{file_name}')
df_verified = pd.read_csv(f'./data/verified_{file_name}')

threshold = 86

df = df[df['frequency'] > threshold]
# Measurement
measurement = 'device'  # Choose the measurement to plot ('device', 'app', 'os', 'channel')

# Count occurrences of each measurement in both datasets
measurement_counts_all = df[measurement].value_counts()
measurement_counts_attributed = df_verified[measurement].value_counts()

measurement_counts_all = measurement_counts_all[measurement_counts_all > 5]
measurement_counts_attributed = measurement_counts_attributed[measurement_counts_attributed > 155]

print(measurement_counts_all.head(20))
print(measurement_counts_attributed.head(20))

# Plotting
fig, axes = plt.subplots(2, 1, figsize=(14, 12))

# Plot for all data after frequency filtering
if not measurement_counts_all.empty:
    measurement_counts_all.plot(kind='bar', color='blue', alpha=0.7, ax=axes[0])
    axes[0].set_title(f'Total Count of {measurement} Based on Frequency Threshold = {threshold} (All Data)')
    axes[0].set_xlabel(f'{measurement}')
    axes[0].set_ylabel('Count')
    axes[0].grid(axis='y')
    axes[0].tick_params(axis='x', rotation=0)

    # Add labels on top of each bar for all data
    for p in axes[0].patches:
        if p.get_height() > 0:  # Add label only if count is greater than 0
            axes[0].annotate(
                str(int(p.get_height())),
                (p.get_x() + p.get_width() / 2., p.get_height()),
                ha='center', va='bottom',
                fontsize=10, color='black', xytext=(0, 5),
                textcoords='offset points'
            )
else:
    axes[0].text(0.5, 0.5, 'No data available for the selected threshold (All Data)', 
                 horizontalalignment='center', verticalalignment='center', transform=axes[0].transAxes, fontsize=12)
    axes[0].set_axis_off()

# Plot for data where is_attributed == 1 (without frequency filtering)
if not measurement_counts_attributed.empty:
    measurement_counts_attributed.plot(kind='bar', color='green', alpha=0.7, ax=axes[1])
    axes[1].set_title(f'Total Count of {measurement} for is_attributed = 1')
    axes[1].set_xlabel(f'{measurement}')
    axes[1].set_ylabel('Count')
    axes[1].grid(axis='y')
    axes[1].tick_params(axis='x', rotation=0)

    # Add labels on top of each bar for data with is_attributed = 1
    for p in axes[1].patches:
        if p.get_height() > 0:  # Add label only if count is greater than 0
            axes[1].annotate(
                str(int(p.get_height())),
                (p.get_x() + p.get_width() / 2., p.get_height()),
                ha='center', va='bottom',
                fontsize=10, color='black', xytext=(0, 5),
                textcoords='offset points'
            )
else:
    axes[1].text(0.5, 0.5, 'No data available for is_attributed = 1', 
                 horizontalalignment='center', verticalalignment='center', transform=axes[1].transAxes, fontsize=12)
    axes[1].set_axis_off()

# Adjust layout to avoid overlap
plt.tight_layout()

# Show the plot
plt.show()

