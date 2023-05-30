import os
import pandas as pd


def count_records(file_path):
    df = pd.read_csv(file_path)
    record_count = len(df)
    return record_count


def count_records_in_folder(folder_path):
    file_records = {}

    files = os.listdir(folder_path)

    for file in files:
        file_path = os.path.join(folder_path, file)
        if file.endswith('.csv'):
            record_count = count_records(file_path)
            file_records[file] = record_count

    return file_records


# Example usage
folder_path = 'result-of-IMIS-with-fairtrading-and-certificates'
file_records = count_records_in_folder(folder_path)

for file, count in file_records.items():
    print(f"{file}: Total record count: {count}")
