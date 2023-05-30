import pandas as pd
import config as _config
import filter_functions
from concurrent.futures import ThreadPoolExecutor
import io
import boto3


def lambda_handler(event, context):
    """
    Lambda function handler to process dataframes, merge data,
    and upload results to S3.

    Reads three CSV files containing data on Fairtrade certificates,
    licenses, and a report from the NSW government.
    Performs data preprocessing steps on the individual and
    certificate dataframes, merges them with the NSW report
    based on matching fields, and uploads the resulting dataframes
    to separate CSV files in an S3 bucket.
    """

    # Read input CSV files from S3
    s3 = boto3.client('s3')
    individual_file = s3.get_object(
        Bucket=_config.S3_BUCKET_NAME, Key=_config.INDIVIDUAL_FILE_KEY)
    certificate_file = s3.get_object(
        Bucket=_config.S3_BUCKET_NAME, Key=_config.CERTIFICATE_FILE_KEY)
    reinsw_file = s3.get_object(
        Bucket=_config.S3_BUCKET_NAME, Key=_config.REINSW_FILE_KEY)
    individual_df = pd.read_csv(individual_file['Body'])
    certificate_df = pd.read_csv(certificate_file['Body'])
    reinsw_df = pd.read_csv(reinsw_file['Body'])

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

    # Upload merged results to S3
    s3_resource = boto3.resource('s3')
    output_bucket = s3_resource.Bucket(_config.S3_BUCKET_NAME)

    with ThreadPoolExecutor() as executor:
        for result_name, result_df in merged_cert_results.items():
            file_contents = result_df.to_csv(index=False)
            file_obj = io.BytesIO(file_contents.encode())
            output_bucket.upload_fileobj(
                file_obj, f"result_cer_reinsw/{result_name}.csv")

        for result_name, result_df in merged_inv_results.items():
            file_contents = result_df.to_csv(index=False)
            file_obj = io.BytesIO(file_contents.encode())
            output_bucket.upload_fileobj(
                file_obj, f"result_inv_reinsw/{result_name}.csv")
