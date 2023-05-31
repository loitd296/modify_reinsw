import pandas as pd
import config as _config
import os


def process_dataframes_and_save_results(individual_paths, certificate_paths, result_folder):
    """
    Processes dataframes, merges data, and saves the results to CSV files.

    Args:
    individual_paths: List of paths to individual data CSV files.
    certificate_paths: List of paths to certificate data CSV files.
    result_folder: The folder where the result CSV files will be saved.
    """
    for ind_path, cert_path in zip(individual_paths, certificate_paths):
        ind_df = pd.read_csv(ind_path)
        cert_df = pd.read_csv(cert_path)
        result_df = pd.concat([ind_df, cert_df])

        file_name = os.path.basename(ind_path)
        result_file_path = os.path.join(result_folder, file_name)
        result_df.to_csv(result_file_path, index=False)


def main():
    """
    Main function to process dataframes, merge data, 
    and write results to CSV files.

    Reads several CSV files containing data on Fairtrade individual licenses
    and certificate licenses. Concatenates and merges the records from these files,
    and writes the resulting dataframes to separate CSV files.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    individual_paths = [
        _config.S3_INDIVIDUAL_LICNUM,
        _config.S3_INDIVIDUAL_LICNUM_LIC,
        _config.S3_INDIVIDUAL_LICNUM_LIC_FNAME_LNAME,
        _config.S3_INDIVIDUAL_FULL_CONDITION,
        _config.S3_INDIVIDUAL_LIC,
        _config.S3_INDIVIDUAL_LIC_FNAME_LNAME,
        _config.S3_INDIVIDUAL_LIC_FNAME_LNAME_ADDRESS
    ]

    certificate_paths = [
        _config.S3_CERTIFICATE_LICNUM,
        _config.S3_CERTIFICATE_LICNUM_LIC,
        _config.S3_CERTIFICATE_LICNUM_LIC_FNAME_LNAME,
        _config.S3_CERTIFICATE_FULL_CONDITION,
        _config.S3_CERTIFICATE_LIC,
        _config.S3_CERTIFICATE_LIC_FNAME_LNAME,
        _config.S3_CERTIFICATE_LIC_FNAME_LNAME_ADDRESS
    ]

    result_folder = "result-individual-and-certificates"

    process_dataframes_and_save_results(
        individual_paths, certificate_paths, result_folder)


if __name__ == "__main__":
    main()
