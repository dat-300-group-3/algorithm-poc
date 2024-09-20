import csv

# Define the input and output file paths
input_csv_path = './test.csv'
output_csv_path = './test_split.csv'

# Define the number of lines to keep
line_count = 10000000

# Open the input and output CSV files
with open(input_csv_path, 'r') as input_file, open(output_csv_path, 'w', newline='') as output_file:
    reader = csv.reader(input_file)
    writer = csv.writer(output_file)
    
    # Write the header
    header = next(reader)
    writer.writerow(header)
    
    # Write the first 10 million lines
    for i, row in enumerate(reader):
        if i >= (line_count - 1):  # -1 because header is already written
            break
        writer.writerow(row)

print(f"Created a new CSV file with the first {line_count} lines at {output_csv_path}")
