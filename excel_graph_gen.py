import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
from utils.app_constant import TRAIN_FILE, SAMPLE_FILE


class DataStats:
    def __init__(self) -> None:
        self.df = dd.read_csv(SAMPLE_FILE)
        self.df['click_time'] = dd.to_datetime(self.df['click_time'])
        self.df['date'] = self.df['click_time'].dt.date

    def save_to_excel(self, writer, data, sheet_name):
        """Helper method to save DataFrame to Excel sheet."""
        if isinstance(data, pd.Series):
            data = data.to_frame()  # Convert Series to DataFrame to write to Excel
        data.to_excel(writer, sheet_name=sheet_name)

    def save_plot(self, data, column_name, plot_name):
        """Helper method to generate and save plot as an image."""
        plt.figure(figsize=(10, 6))
        data.plot(kind='bar')
        plt.title(f'{column_name} Distribution')
        plt.ylabel('Count')
        plt.tight_layout()
        # Save the plot to a file
        plot_filename = f'{plot_name}.png'
        plt.savefig(plot_filename)
        plt.close()  # Close the plot to free up memory
        return plot_filename

    def insert_plot_into_excel(self, writer, sheet_name, plot_filename):
        """Insert saved plot image into the Excel sheet."""
        worksheet = writer.sheets[sheet_name]
        # Define where to insert the image in the worksheet (row 1, column 1)
        worksheet.insert_image('H2', plot_filename)

    def count_click_by_date(self, writer):
        row_count_by_date = self.df.groupby('date').size().compute()
        self.save_to_excel(writer, row_count_by_date, 'clicks by date')
        # Save plot for clicks by date
        plot_filename = self.save_plot(row_count_by_date, 'Date', 'clicks_by_date')
        self.insert_plot_into_excel(writer, 'clicks by date', plot_filename)

    def is_attributed_stats_generator(self, writer):
        row_count_by_date_and_is_attributed = self.df.groupby(['date', 'is_attributed']).size().compute().unstack(fill_value=0)
        self.save_to_excel(writer, row_count_by_date_and_is_attributed, 'is attributed by date')
        # Save plot for is_attributed by date
        plot_filename = self.save_plot(row_count_by_date_and_is_attributed, 'Is Attributed', 'is_attributed_by_date')
        self.insert_plot_into_excel(writer, 'is attributed by date', plot_filename)

    def generate_stats(self, writer, column_name, group_count_sheet, total_count_sheet, value_count_sheet):
        """Generalized method to generate and save statistics for a given column."""
        
        # Group by 'date' and the given column (e.g., 'app' or 'device'), count occurrences, and unstack
        group_count = self.df.groupby(['date', column_name]).size().compute().unstack(fill_value=0)
        self.save_to_excel(writer, group_count, group_count_sheet)
        
        # Save plot for group count by date
        plot_filename = self.save_plot(group_count, column_name, f'{column_name}_count_by_date')
        self.insert_plot_into_excel(writer, group_count_sheet, plot_filename)

        # Count unique values in the given column
        unique_count = self.df[column_name].nunique().compute()
        unique_count_df = pd.DataFrame({f'unique_{column_name}_count': [unique_count]})
        self.save_to_excel(writer, unique_count_df, total_count_sheet)

        # Count occurrences of each unique element in the given column
        value_counts = self.df[column_name].value_counts().compute()
        self.save_to_excel(writer, value_counts, value_count_sheet)

        # Save plot for value counts
        plot_filename = self.save_plot(value_counts, column_name, f'{column_name}_value_counts')
        self.insert_plot_into_excel(writer, value_count_sheet, plot_filename)


if __name__ == "__main__":
    # Create an Excel writer object
    with pd.ExcelWriter('data_statistics.xlsx', engine='xlsxwriter') as writer:
        stats = DataStats()
        
        # Save each calculation in a different sheet of the same Excel file
        stats.count_click_by_date(writer)
        stats.is_attributed_stats_generator(writer)
        stats.generate_stats(writer, 
                             column_name='app', 
                             group_count_sheet='app count by date', 
                             total_count_sheet='total app counts', 
                             value_count_sheet='clicks against app ID')
        
        # Generate device statistics
        stats.generate_stats(writer, 
                             column_name='device', 
                             group_count_sheet='device count by date', 
                             total_count_sheet='total device counts', 
                             value_count_sheet='clicks against device ID')
        
        stats.generate_stats(writer, 
                             column_name='os', 
                             group_count_sheet='os count by date', 
                             total_count_sheet='total os counts', 
                             value_count_sheet='clicks against os ID')

        stats.generate_stats(writer, 
                             column_name='channel', 
                             group_count_sheet='channel count by date', 
                             total_count_sheet='total channel counts', 
                             value_count_sheet='clicks against channel ID')
