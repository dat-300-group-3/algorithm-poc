
from datasketches import frequent_strings_sketch, frequent_items_error_type

import numpy as np
import pandas as pd

# import data
file_path = './data/original/test_split.csv'
data = pd.read_csv(file_path)
print(data.head())
print("Data loaded")
print("________________________")
print(len(data))



# Create a new Frequent Items sketch
k = 64
sketch = frequent_strings_sketch(k)



# Update the sketch with the data
print("Updating sketch")
for index, row in data.iterrows():
    sketch.update(str(row['ip'] + row['device']))
    

print("________________________")
# Query the sketch


print("Querying sketch")
no_false_positives = sketch.get_frequent_items(frequent_items_error_type.NO_FALSE_POSITIVES)
length = len(no_false_positives)
treshold = 0.1 * length
print(length)

#print("No false positives: ", no_false_positives)

#no_false_negatives = sketch.get_frequent_items(frequent_items_error_type.NO_FALSE_NEGATIVES)
#print("No false negatives: ", no_false_negatives)
#print(len(no_false_negatives))

result = [(tuple[0],tuple[1]) for tuple in no_false_positives if tuple[1] > treshold]
print(result)
print(len(result))  
print(sketch)