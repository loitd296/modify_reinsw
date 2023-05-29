import pandas as pd
import config as _config
import filter_functions
from concurrent.futures import ThreadPoolExecutor


def main():
    """
    Main function to process dataframes, merge data,
    and write results to CSV files.

    Reads three CSV files containing data on Fairtrade certificates
    licenses, and a report from the NSW government.
    Performs data preprocessing steps on the individual and
    certificate dataframes, merges them with the NSW report
    based on matching fields, and writes the resulting dataframes
    to separate CSV files.
    """

    # Read input CSV files
    individual_df = pd.read_csv(_config.INDIVIDUAL_ORIGINAL_PATH)
    certificate_df = pd.read_csv(_config.CERTIFICATE_ORIGINAL_PATH)
    reinsw_df = pd.read_csv(_config.REINSW_PATH)

    # Preprocess dataframes
    with ThreadPoolExecutor() as executor:
        individual_df = executor.submit(
            filter_functions.preprocess_lname_fname_address_rename_column_df, individual_df)
        certificate_df = executor.submit(
            filter_functions.preprocess_lname_fname_address_rename_column_df, certificate_df)
        individual_df = individual_df.result()
        certificate_df = certificate_df.result()

    # Merge dataframes with reinsw_df
    merged_cert_results = filter_functions.merge_cert_dataframes(
        certificate_df, reinsw_df)
    merged_inv_results = filter_functions.merge_inv_dataframes(
        individual_df, reinsw_df)

    # Write merged results to separate CSV files
    output_directory_cert = "result_cer_reinsw/"
    output_directory_inv = "result_inv_reinsw/"

    with ThreadPoolExecutor() as executor:
        for result_name, result_df in merged_cert_results.items():
            file_path = f"{output_directory_cert}{result_name}.csv"
            executor.submit(result_df.to_csv, file_path, index=False)

        for result_name, result_df in merged_inv_results.items():
            file_path = f"{output_directory_inv}{result_name}.csv"
            executor.submit(result_df.to_csv, file_path, index=False)


if __name__ == "__main__":
    main()
