import pandas as pd
import config as _config
import boto3
import os
import io


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
    s3_client = boto3.client('s3')
    bucket_name = _config.S3_BUCKET_NAME

    # Get CSV files from S3
    individual_lic_num_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_INDIVIDUAL_LICNUM)
    certificate_lic_num_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_CERTIFICATE_LICNUM)
    individual_lic_and_lic_num_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_INDIVIDUAL_LICNUM_LIC)
    certificate_lic_and_lic_num_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_CERTIFICATE_LICNUM_LIC)
    individual_lic_and_lic_num_and_fname_and_lname_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_INDIVIDUAL_LICNUM_LIC_FNAME_LNAME)
    certificate_lic_and_lic_num_and_fname_and_lname_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_CERTIFICATE_LICNUM_LIC_FNAME_LNAME)
    individual_full_condition_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_INDIVIDUAL_FULL_CONDITION)
    certificate_full_condition_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_CERTIFICATE_FULL_CONDITION)
    individual_lic_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_INDIVIDUAL_LIC)
    certificate_lic_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_CERTIFICATE_LIC)
    individual_lic_and_fname_and_lname_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_INDIVIDUAL_LIC_FNAME_LNAME)
    certificate_lic_and_fname_and_lname_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_CERTIFICATE_LIC_FNAME_LNAME)
    individual_lic_and_fname_and_lname_and_address_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_INDIVIDUAL_LIC_FNAME_LNAME_ADDRESS)
    certificate_lic_and_fname_and_lname_and_address_obj = s3_client.get_object(
        Bucket=bucket_name, Key=_config.S3_CERTIFICATE_LIC_FNAME_LNAME_ADDRESS)

    # Convert csv files to dataframes
    individual_lic_num_df = pd.read_csv(individual_lic_num_obj['Body'])
    certificate_lic_num_df = pd.read_csv(certificate_lic_num_obj['Body'])
    individual_lic_and_lic_num_df = pd.read_csv(
        individual_lic_and_lic_num_obj['Body'])
    certificate_lic_and_lic_num_df = pd.read_csv(
        certificate_lic_and_lic_num_obj['Body'])
    individual_lic_and_lic_num_and_fname_and_lname_df = pd.read_csv(
        individual_lic_and_lic_num_and_fname_and_lname_obj['Body'])
    certificate_lic_and_lic_num_and_fname_and_lname_df = pd.read_csv(
        certificate_lic_and_lic_num_and_fname_and_lname_obj['Body'])
    individual_full_condition_df = pd.read_csv(
        individual_full_condition_obj['Body'])
    certificate_full_condition_df = pd.read_csv(
        certificate_full_condition_obj['Body'])
    individual_lic_df = pd.read_csv(individual_lic_obj['Body'])
    certificate_lic_df = pd.read_csv(certificate_lic_obj['Body'])
    individual_lic_and_fname_and_lname_df = pd.read_csv(
        individual_lic_and_fname_and_lname_obj['Body'])
    certificate_lic_and_fname_and_lname_df = pd.read_csv(
        certificate_lic_and_fname_and_lname_obj['Body'])
    individual_lic_and_fname_and_lname_and_address_df = pd.read_csv(
        individual_lic_and_fname_and_lname_and_address_obj['Body'])
    certificate_lic_and_fname_and_lname_and_address_df = pd.read_csv(
        certificate_lic_and_fname_and_lname_and_address_obj['Body'])

    results = {
        'result_on_lic_num': pd.concat([individual_lic_num_df, certificate_lic_num_df]),
        'result_on_lic_num_lic': pd.concat([individual_lic_and_lic_num_df, certificate_lic_and_lic_num_df]),
        'result_on_lic_num_lic_lname_fname': pd.concat([individual_lic_and_lic_num_and_fname_and_lname_df, certificate_lic_and_lic_num_and_fname_and_lname_df]),
        'result_on_full_condition': pd.concat([individual_full_condition_df, certificate_full_condition_df]),
        'result_on_lic': pd.concat([individual_lic_df, certificate_lic_df]),
        'result_on_lic_lname_fname': pd.concat([individual_lic_and_fname_and_lname_df, certificate_lic_and_fname_and_lname_df]),
        'result_on_lic_lname_fname_address': pd.concat([individual_lic_and_fname_and_lname_and_address_df, certificate_lic_and_fname_and_lname_and_address_df])
    }

    # Upload the resulting dataframes to S3
    for i, (result_name, result_df) in enumerate(results.items()):
        csv_buffer = io.StringIO()
        result_df.to_csv(csv_buffer, index=False)
        file_name = f'{i+1}_{result_name}.csv'
        s3_client.put_object(Body=csv_buffer.getvalue(),
                             Bucket=bucket_name, Key=f'result-individual-and-certificates/{file_name}')

    return {
        'statusCode': 200,
        'body': 'Data processing and upload to S3 completed successfully.'
    }
