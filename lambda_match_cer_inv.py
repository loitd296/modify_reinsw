import pandas as pd
import config as _config
import boto3
import io


def get_dataframe_from_s3(s3_client, bucket_name, key):
    """
    Fetches an object from an S3 bucket and converts it to a pandas DataFrame.

    Args:
    s3_client: boto3 S3 client object.
    bucket_name: The name of the S3 bucket.
    key: The key (path) of the object within the S3 bucket.

    Returns:
    A pandas DataFrame containing the data from the S3 object.
    """
    s3_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    return pd.read_csv(io.BytesIO(s3_obj['Body'].read()))


def upload_dataframe_to_s3(s3_client, df, bucket_name, key):
    """
    Converts a pandas DataFrame to a CSV and uploads it to an S3 bucket.

    Args:
    s3_client: boto3 S3 client object.
    df: The pandas DataFrame to be uploaded.
    bucket_name: The name of the S3 bucket.
    key: The key (path) where the object will be stored within the S3 bucket.
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Body=csv_buffer.getvalue(),
                         Bucket=bucket_name, Key=key)


def lambda_handler(event, context):
    """
    Lambda function handler that processes dataframes, merges data, and uploads
    the results to an S3 bucket.

    Args:
    event: AWS Lambda uses this parameter to pass in event data to the handler.
    context: AWS Lambda uses this parameter to provide runtime information to the handler.

    Returns:
    A dictionary with information about the operation's status.
    """
    s3_client = boto3.client('s3')
    bucket_name = _config.S3_BUCKET_NAME

    # List of S3 keys (paths)
    individual_keys = [_config.S3_INDIVIDUAL_LICNUM, _config.S3_INDIVIDUAL_LICNUM_LIC,
                       _config.S3_INDIVIDUAL_LICNUM_LIC_FNAME_LNAME, _config.S3_INDIVIDUAL_FULL_CONDITION,
                       _config.S3_INDIVIDUAL_LIC, _config.S3_INDIVIDUAL_LIC_FNAME_LNAME,
                       _config.S3_INDIVIDUAL_LIC_FNAME_LNAME_ADDRESS]

    certificate_keys = [_config.S3_CERTIFICATE_LICNUM, _config.S3_CERTIFICATE_LICNUM_LIC,
                        _config.S3_CERTIFICATE_LICNUM_LIC_FNAME_LNAME, _config.S3_CERTIFICATE_FULL_CONDITION,
                        _config.S3_CERTIFICATE_LIC, _config.S3_CERTIFICATE_LIC_FNAME_LNAME,
                        _config.S3_CERTIFICATE_LIC_FNAME_LNAME_ADDRESS]

    # Ensure that there is the same number of individual and certificate keys
    assert len(individual_keys) == len(certificate_keys), \
        "There should be the same number of individual and certificate keys!"

    # The names of the result datasets
    result_names = ['result_on_lic_num', 'result_on_lic_num_lic', 'result_on_lic_num_lic_lname_fname',
                    'result_on_full_condition', 'result_on_lic', 'result_on_lic_lname_fname',
                    'result_on_lic_lname_fname_address']

    # Iterate over the keys and process the data
    for i, (ind_key, cert_key, result_name) in enumerate(zip(individual_keys, certificate_keys, result_names)):
        ind_df = get_dataframe_from_s3(s3_client, bucket_name, ind_key)
        cert_df = get_dataframe_from_s3(s3_client, bucket_name, cert_key)

        # Concatenate the two dataframes
        result_df = pd.concat([ind_df, cert_df])

        # Prepare the key for the upload
        file_key = f'result-individual-and-certificates/{i+1}_{result_name}.csv'

        # Upload the result dataframe to S3
        upload_dataframe_to_s3(s3_client, result_df, bucket_name, file_key)

    return {
        'statusCode': 200,
        'body': 'Data processing and upload to S3 completed successfully.'
    }
