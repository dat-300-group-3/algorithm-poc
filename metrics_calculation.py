import pandas as pd

# Load the CSV files
realcount_df = pd.read_csv('above_rulebased_threshold_realcount.csv')
predicted_df = pd.read_csv('above_rulebased_threshold_w1000000_10d.csv')
#predicted_df = pd.read_csv('above_threshold_identifiers_w59M_5d.csv')

# Extract sets of identifiers from both dataframes
realcount_identifiers = set(realcount_df['identifier'])
predicted_identifiers = set(predicted_df['identifier'])

# Calculate True Positives (TP), False Positives (FP), and False Negatives (FN)
true_positives = realcount_identifiers.intersection(predicted_identifiers)
false_positives = predicted_identifiers - true_positives
false_negatives = realcount_identifiers - true_positives

# Calculate accuracy, precision, and recall
TP = len(true_positives)
FP = len(false_positives)
FN = len(false_negatives)
TN = 0  # True negatives are not clearly defined in this context without the full set of possible identifiers

precision = TP / (TP + FP) if (TP + FP) > 0 else 0
recall = TP / (TP + FN) if (TP + FN) > 0 else 0
accuracy = TP / (TP + FP + FN) if (TP + FP + FN) > 0 else 0

print(f'True Positives: {TP}')
print(f'False Positives: {FP}')
print(f'False Negatives: {FN}')
#print(f'True Negatives: {TN}')
print(f'Precision: {precision}')
print(f'Recall: {recall}')
print(f'Accuracy: {accuracy}')
