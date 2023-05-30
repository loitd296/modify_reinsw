Project Files and Features
This repository contains the following files, each serving a specific purpose in your project:

1. config.py
This file contains configuration variables used by other modules in your project. It includes file paths, output paths, and CSV headers.

Features:

Configuration variables for file paths, output paths, and CSV headers.
2. countfileexcel.py
This module provides functions to count the number of records in CSV files. It includes functions to count records in a single file and count records in a folder containing multiple files.

Features:

Function to count records in a single CSV file.
Function to count records in a folder containing multiple CSV files.
3. filter_functions.py
This module contains various functions used for data filtering and merging. It includes functions to parse addresses, extract first and last names from licensee names, and merge dataframes based on different matching conditions.

Features:

Functions for data filtering and merging.
Parsing addresses and extracting names from licensee names.
4. lambda_function.py
This file contains the main Lambda function handler that processes dataframes, merges data, and uploads results to an S3 bucket. It reads CSV files from an S3 bucket, preprocesses the individual and certificate dataframes, merges them with the NSW report, and uploads the resulting dataframes to separate CSV files in S3.

Features:

Lambda function handler for processing dataframes.
Preprocessing, merging, and uploading results to an S3 bucket.
5. main.py
This file contains the main function that processes dataframes, merges data, and writes results to CSV files. It reads CSV files locally, preprocesses the individual and certificate dataframes, merges them with the NSW report based on matching fields, and writes the resulting dataframes to separate CSV files.

Features:

Main function for processing dataframes.
Local CSV file processing, merging, and writing results to CSV files.
6. match_cer_inv.py
This file contains the main function that processes several CSV files containing data on Fairtrade individual licenses and certificate licenses. It concatenates and merges the records from these files based on specific conditions and writes the resulting dataframes to separate CSV files.

Features:

Main function for processing CSV files.
Concatenating and merging records based on specific conditions.
Files and their Input/Output
config.py

Input: No specific input. It contains constant values and file paths used in other files.
Output: No specific output. It is imported and referenced by other files for configuration purposes.
countfileexcel.py

Input: Takes a file path as input, which is the path to a CSV file.
Output: Returns a dictionary where the keys are the file names and the values are the count of records in each file.
filter_functions.py

Input: Various functions in this file accept pandas DataFrames as input, along with other parameters.
Output: The functions in this file return processed or merged pandas DataFrames based on the input.
lambda_function.py

Input: This file is intended to be used as an AWS Lambda function. It expects input data in the form of CSV files stored in an S3 bucket. The input CSV files include individual_file, certificate_file, and reinsw_file.
Output: The merged results of the individual and certificate dataframes are uploaded as separate CSV files to the specified S3 bucket.
main.py

Input: Reads three CSV files: individual.csv, certificate.csv, and reinsw_report.csv.
Output: Writes the merged results of the individual and certificate dataframes to separate CSV files in the result_cer_reinsw and result_inv_reinsw folders.
match_cer_inv.py

Input: Reads multiple CSV files related to individual and certificate licenses.
Output: Writes the merged results of the individual and certificate dataframes to separate CSV files in the result-individual-and-certificates folder.
