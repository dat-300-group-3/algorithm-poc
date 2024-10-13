import pandas as pd
import powerlaw
import matplotlib.pyplot as plt
from scipy.stats import chisquare


class DataLoader:
    def __init__(self):
        #self.file_name = file_name
        self.df = None
        self.df_verified = None

    def load_data(self):
        self.df = pd.read_csv(f'data/freq_potential_fraud_users.csv')
        self.df_verified = pd.read_csv(f'data/freq_genuine_users.csv')
        return self.df, self.df_verified


class PowerLawAnalyzer:
    def __init__(self, df, df_verified):
        self.df = df
        self.df_verified = df_verified
        self.threshold = None

    def fit_power_law(self, frequency_data):
        fit = powerlaw.Fit(frequency_data, verbose=False)
        alpha = fit.alpha
        xmin = fit.xmin
        return alpha, xmin

    def calculate_global_threshold(self, percentile=1 - 0.0017195426):
        threshold = self.df['frequency'].quantile(percentile)
        self.threshold = threshold
        return threshold

    def filter_by_threshold(self):
        df_t = self.df[self.df['frequency'] >= self.threshold]
        return df_t


class AnomalyDetector:
    def __init__(self, df_t, df_verified, expected_ratio=2):
        self.df_t = df_t
        self.df_verified = df_verified
        self.expected_ratio = expected_ratio
        self.anomalies = []
        self.anomalous_values = []

    def calculate_value_counts(self):
        t_values_list = []
        is_attributed_values_list = []

        for column in ['app', 'device', 'os', 'channel']:
            t_values = self.df_t[column].value_counts().to_dict()
            t_values_list.append({column: t_values})

            is_attributed_values = self.df_verified[column].value_counts().to_dict()
            is_attributed_values_list.append({column: is_attributed_values})

        return t_values_list, is_attributed_values_list

    def perform_chi_square(self, t_values_list, is_attributed_values_list):
        for i, column in enumerate(['app', 'device', 'os', 'channel']):
            t_values = t_values_list[i][column]
            is_attributed_values = is_attributed_values_list[i][column]

            all_keys = set(t_values.keys()).union(set(is_attributed_values.keys()))
            t_frequencies = [t_values.get(key, 0) for key in all_keys]
            is_attributed_frequencies = [is_attributed_values.get(key, 0) for key in all_keys]

            final_filtered_keys = [
                key for key, t_freq, exp_freq in zip(all_keys, t_frequencies, is_attributed_frequencies)
                if t_freq > exp_freq
            ]
            t_frequencies = [t_values.get(key, 0) for key in final_filtered_keys]
            is_attributed_frequencies = [is_attributed_values.get(key, 0) for key in final_filtered_keys]

            total_t_frequencies = sum(t_frequencies)
            total_is_attributed_frequencies = sum(is_attributed_frequencies)

            if total_t_frequencies == 0 or total_is_attributed_frequencies == 0:
                continue

            t_frequencies = [freq * (total_is_attributed_frequencies / total_t_frequencies) for freq in t_frequencies]
            is_attributed_frequencies = [max(freq, 1e-10) for freq in is_attributed_frequencies]

            chi2, p = chisquare(f_obs=t_frequencies, f_exp=is_attributed_frequencies)

            if p < 0.01:
                self.anomalies.append({
                    'column': column,
                    'chi2': chi2,
                    'p_value': p,
                    't_frequencies': t_frequencies,
                    'is_attributed_frequencies': is_attributed_frequencies
                })

                column_anomalous_values = []
                for j, key in enumerate(final_filtered_keys):
                    diff = (t_frequencies[j] - is_attributed_frequencies[j])
                    if (diff > self.expected_ratio * is_attributed_frequencies[j]) & (diff > 3):
                        column_anomalous_values.append({
                            'value': key,
                            'observed': t_frequencies[j],
                            'expected': is_attributed_frequencies[j],
                            'difference': diff
                        })

                self.anomalous_values.append({
                    'column': column,
                    'anomalous_values': column_anomalous_values
                })

        return self.anomalous_values


class ReportGenerator:
    def __init__(self, file_name, anomalies, anomaly_thresholds):
        self.file_name = file_name
        self.anomalies = anomalies
        self.anomaly_thresholds = anomaly_thresholds

    def print_anomalies(self):
        for column_values in self.anomalies:
            print(f"\nAnomalous values in column: {column_values['column']}")
            for value_info in column_values['anomalous_values']:
                print(f"Value: {value_info['value']}, Observed: {value_info['observed']}, "
                      f"Expected: {value_info['expected']}, Difference: {value_info['difference']}")

    def save_anomalies_to_csv(self):
        anomaly_file_path = f'anomalous_values_{self.file_name}'
        with open(anomaly_file_path, 'w') as file:
            file.write('column,value,observed,expected,difference\n')
            for column_values in self.anomalies:
                for value_info in column_values['anomalous_values']:
                    file.write(f"{column_values['column']},{value_info['value']},"
                               f"{value_info['observed']},{value_info['expected']},{value_info['difference']}\n")

    def save_thresholds_to_csv(self):
        anomaly_thresholds_df = pd.DataFrame(self.anomaly_thresholds)
        anomaly_thresholds_df.to_csv(f'anomalous_thresholds_{self.file_name}', index=False)

    def display_thresholds(self):
        anomaly_thresholds_df = pd.DataFrame(self.anomaly_thresholds)
        print(anomaly_thresholds_df)


# --- Main code execution ---

# Initialize file name
file_name = 'w1000000_10d.csv'

# Load data
data_loader = DataLoader()
df, df_verified = data_loader.load_data()

# Perform power-law analysis
power_law_analyzer = PowerLawAnalyzer(df, df_verified)
df_alpha, df_threshold = power_law_analyzer.fit_power_law(df['frequency'])
alpha_verified, threshold_verified = power_law_analyzer.fit_power_law(df_verified['frequency'])

# Calculate global threshold
global_threshold = power_law_analyzer.calculate_global_threshold()
df_t = power_law_analyzer.filter_by_threshold()

# Detect anomalies
anomaly_detector = AnomalyDetector(df_t, df_verified)
t_values_list, is_attributed_values_list = anomaly_detector.calculate_value_counts()
anomalous_values = anomaly_detector.perform_chi_square(t_values_list, is_attributed_values_list)

# Report and save anomalies
report_generator = ReportGenerator(file_name, anomalous_values, [])
report_generator.print_anomalies()
report_generator.save_anomalies_to_csv()
report_generator.display_thresholds()
