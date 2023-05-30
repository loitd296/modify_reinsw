import pandas as pd
import config as _config


def main():
    """
    Main function to process dataframes, merge data, 
    and write results to CSV files.

    Reads several CSV files containing data on Fairtrade individual licenses
    and certificate licenses. Concatenates and merges the records from these files
    based on specific conditions, and writes the resulting dataframes to separate CSV files.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    individual_lic_num_df = pd.read_csv(_config.S3_INDIVIDUAL_LICNUM)
    certificate_lic_num_df = pd.read_csv(_config.S3_CERTIFICATE_LICNUM)
    individual_lic_and_lic_num_df = pd.read_csv(
        _config.S3_INDIVIDUAL_LICNUM_LIC)
    certificate_lic_and_lic_num_df = pd.read_csv(
        _config.S3_CERTIFICATE_LICNUM_LIC)
    individual_lic_and_lic_num_and_fname_and_lname_df = pd.read_csv(
        _config.S3_INDIVIDUAL_LICNUM_LIC_FNAME_LNAME)
    certificate_lic_and_lic_num_and_fname_and_lname_df = pd.read_csv(
        _config.S3_CERTIFICATE_LICNUM_LIC_FNAME_LNAME)
    individual_full_condition_df = pd.read_csv(
        _config.S3_INDIVIDUAL_FULL_CONDITION)
    certificate_full_condition_df = pd.read_csv(
        _config.S3_CERTIFICATE_FULL_CONDITION)
    individual_lic_df = pd.read_csv(_config.S3_INDIVIDUAL_LIC)
    certificate_lic_df = pd.read_csv(_config.S3_CERTIFICATE_LIC)
    individual_lic_and_fname_and_lname_df = pd.read_csv(
        _config.S3_INDIVIDUAL_LIC_FNAME_LNAME)
    certificate_lic_and_fname_and_lname_df = pd.read_csv(
        _config.S3_CERTIFICATE_LIC_FNAME_LNAME)
    individual_lic_and_fname_and_lname_and_address_df = pd.read_csv(
        _config.S3_INDIVIDUAL_LIC_FNAME_LNAME_ADDRESS)
    certificate_lic_and_fname_and_lname_and_address_df = pd.read_csv(
        _config.S3_CERTIFICATE_LIC_FNAME_LNAME_ADDRESS)

    results = {
        'result_on_lic_num': pd.concat([individual_lic_num_df, certificate_lic_num_df]),
        'result_on_lic_num_lic': pd.concat([individual_lic_and_lic_num_df, certificate_lic_and_lic_num_df]),
        'result_on_lic_num_lic_lname_fname': pd.concat([individual_lic_and_lic_num_and_fname_and_lname_df, certificate_lic_and_lic_num_and_fname_and_lname_df]),
        'result_on_full_condition': pd.concat([individual_full_condition_df, certificate_full_condition_df]),
        'result_on_lic': pd.concat([individual_lic_df, certificate_lic_df]),
        'result_on_lic_lname_fname': pd.concat([individual_lic_and_fname_and_lname_df, certificate_lic_and_fname_and_lname_df]),
        'result_on_lic_lname_fname_address': pd.concat([individual_lic_and_fname_and_lname_and_address_df, certificate_lic_and_fname_and_lname_and_address_df])
    }

    for i, (result_name, result_df) in enumerate(results.items()):
        result_df.to_csv(
            f"result-individual-and-certificates/{i+1}_{result_name}.csv", index=False)


if __name__ == "__main__":
    main()
