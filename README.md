# Project Files and Features

This repository contains the following files, each serving a specific purpose in your project:

## 1. config.py

This file contains configuration variables used by other modules in your project. It includes file paths, output paths, and CSV headers.

**Features:**

- Configuration variables for file paths, output paths, and CSV headers.

## 2. countfileexcel.py

This module provides functions to count the number of records in CSV files. It includes functions to count records in a single file and count records in a folder containing multiple files.

**Features:**

- Function to count records in a single CSV file.
- Function to count records in a folder containing multiple CSV files.

## 3. filter_functions.py

This module contains various functions used for data filtering and merging. It includes functions to parse addresses, extract first and last names from licensee names, and merge dataframes based on different matching conditions.

**Features:**

- Functions for data filtering and merging.
- Parsing addresses and extracting names from licensee names.

## 4. lambda_function.py

This file contains the main Lambda function handler that processes dataframes, merges data, and uploads results to an S3 bucket. It reads CSV files from an S3 bucket, preprocesses the individual and certificate dataframes, merges them with the NSW report, and uploads the resulting dataframes to separate CSV files in S3.

**Features:**

- Lambda function handler for processing dataframes.
- Preprocessing, merging, and uploading results to an S3 bucket.

## 5. main.py

This file contains the main function that processes dataframes, merges data, and writes results to CSV files. It reads CSV files locally, preprocesses the individual and certificate dataframes, merges them with the NSW report based on matching fields, and writes the resulting dataframes to separate CSV files.

**Features:**

- Main function for processing dataframes and merging data.
- Reading CSV files locally and writing merged results to separate CSV files.

## 6. match_cer_inv.py

This file contains the main function that processes several CSV files containing data on Fairtrade individual licenses and certificate licenses. It concatenates and merges the records from these files based on specific conditions and writes the resulting dataframes to separate CSV files.

**Features:**

- Main function for processing multiple CSV files.
- Concatenating and merging records based on specific conditions.
- Writing merged results to separate CSV files.
