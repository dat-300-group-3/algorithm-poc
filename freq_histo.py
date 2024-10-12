import dask.dataframe as dd
import matplotlib.pyplot as plt


# Load the CSV file into a Dask DataFrame
#file_name = 'realcount.csv'
#file_name = 'w100000_10d.csv'
file_name = 'w1000000_10d.csv'
#file_name = 'w59M_5d.csv'

ddf = dd.read_csv(f'./data/all_{file_name}')
ddf_verified = dd.read_csv(f'./data/verified_{file_name}')


# Extract the 'frequency' column as a Dask Series for all data
values_all = ddf['frequency'].compute().tolist()
values_verified = ddf_verified['frequency'].compute().tolist()

# Create the plot figure
plt.figure(figsize=(14, 10))

# Plot histogram for all data
plt.subplot(2, 1, 1)
n_all, bins_all, patches_all = plt.hist(values_all, bins=40, color='blue', alpha=0.7)
plt.yscale('log')
bin_centers_all = 0.5 * (bins_all[1:] + bins_all[:-1])
for count, x in zip(n_all, bin_centers_all):
    if count > 0:  # Add label only if count is greater than 0
        plt.text(x, count, f'{int(count)}', ha='center', va='bottom', fontsize=8)
bin_labels_all = [f'{int(bins_all[i])} - {int(bins_all[i+1])}' for i in range(len(bins_all) - 1)]
plt.xticks(bin_centers_all, bin_labels_all, rotation=45, ha='right', fontsize=8)
plt.title('Logarithmic Histogram of all non-verified Users Counts')
plt.xlabel('Click Count Ranges')
plt.ylabel('Frequency of #Users (Log Scale)')
plt.grid(True)

# Plot histogram for data where 'is_attributed' is 1
plt.subplot(2, 1, 2)
n_attr, bins_attr, patches_attr = plt.hist(values_verified, bins=30, color='green', alpha=0.7)
plt.yscale('log')
bin_centers_attr = 0.5 * (bins_attr[1:] + bins_attr[:-1])
for count, x in zip(n_attr, bin_centers_attr):
    if count > 0:  # Add label only if count is greater than 0
        plt.text(x, count, f'{int(count)}', ha='center', va='bottom', fontsize=8)
bin_labels_attr = [f'{int(bins_attr[i])} - {int(bins_attr[i+1])}' for i in range(len(bins_attr) - 1)]
plt.xticks(bin_centers_attr, bin_labels_attr, rotation=45, ha='right', fontsize=8)
plt.title('Logarithmic Histogram of verified buyers Users Counts')
plt.xlabel('Click count ranges')
plt.ylabel('Frequency of #Users(Log Scale)')
plt.grid(True)

# Adjust layout to avoid overlap
plt.tight_layout()

# Show the plot
plt.show()

