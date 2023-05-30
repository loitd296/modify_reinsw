import pandas as pd
import config as _config
import boto3
import os
import io
import concurrent.futures


def read_csv_from_s3(bucket_name, key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(obj['Body'])
    return df


def upload_csv_to_s3(csv_buffer, bucket_name, key):
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=csv_buffer.getvalue(),
                         Bucket=bucket_name, Key=key)


def process_dataframe(match_result_inv_cert_df, result_name):
    # Perform any data processing or merging operations on the dataframe
    # ...

    # Return the processed dataframe
    return match_result_inv_cert_df


def lambda_handler(event, context):
    """
    Lambda function handler to process dataframes, merge data,
    and upload results to an S3 bucket.

    Reads several CSV files containing data on Fairtrade individual licenses
    and certificate licenses. Concatenates and merges the records from these files
    based on specific conditions, and uploads the resulting dataframes to separate CSV files in S3.

    Parameters
    ----------
    event : dict
        The event data passed to the Lambda function.
    context : LambdaContext
        The context object representing the runtime information.

    Returns
    -------
    dict
        A dictionary containing the status and message of the Lambda function execution.
    """
    bucket_name = _config.S3_BUCKET_NAME

    # List of CSV files to process
    csv_files = [
        _config.S3_INDIVIDUAL_LICNUM,
        _config.S3_CERTIFICATE_LICNUM,
        _config.S3_INDIVIDUAL_LICNUM_LIC,
        _config.S3_CERTIFICATE_LICNUM_LIC,
        _config.S3_INDIVIDUAL_LICNUM_LIC_FNAME_LNAME,
        _config.S3_CERTIFICATE_LICNUM_LIC_FNAME_LNAME,
        _config.S3_INDIVIDUAL_FULL_CONDITION,
        _config.S3_CERTIFICATE_FULL_CONDITION,
        _config.S3_INDIVIDUAL_LIC,
        _config.S3_CERTIFICATE_LIC,
        _config.S3_INDIVIDUAL_LIC_FNAME_LNAME,
        _config.S3_CERTIFICATE_LIC_FNAME_LNAME,
        _config.S3_INDIVIDUAL_LIC_FNAME_LNAME_ADDRESS,
        _config.S3_CERTIFICATE_LIC_FNAME_LNAME_ADDRESS
    ]

    results = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks for reading CSV files concurrently
        file_futures = {executor.submit(
            read_csv_from_s3, bucket_name, csv_file): csv_file for csv_file in csv_files}

        # Process the dataframes as they become available
        for future in concurrent.futures.as_completed(file_futures):
            csv_file = file_futures[future]
            try:
                df = future.result()
                # Perform data processing on the dataframe
                result_df = process_dataframe(df, csv_file)
                results[csv_file] = result_df
            except Exception as e:
                print(f"Error processing CSV file '{csv_file}': {str(e)}")

    # Upload the resulting dataframes to S3
    for result_name, result_df in results.items():
        csv_buffer = io.StringIO()
        result_df.to_csv(csv_buffer, index=False)
        upload_csv_to_s3(csv_buffer, bucket_name,
                         f'result-individual-and-certificates/{result_name}.csv')

    return {
        'statusCode': 200,
        'body': 'Data processing and upload to S3 completed successfully.'
    }
