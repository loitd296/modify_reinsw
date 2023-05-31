from typing import List, Tuple
from pandas.api.types import is_numeric_dtype


import pandas as pd

merge_columns_dict = {
    'last_name': ['last_name_fairtrade', 'last_name_reinsw'],
    'first_name': ['first_name_fairtrade', 'first_name_reinsw'],
    'suburb': ['suburb_fairtrade', 'suburb_reinsw'],
    'state': ['state_fairtrade', 'state_reinsw'],
    'post_code': ['post_code_fairtrade', 'post_code_reinsw'],
    'licensee': ['licensee_fairtrade', 'licensee_reinsw']
}


def parse_addresses(addresses: List[str]) -> List[Tuple[str, str, str]]:
    """
    Parses a list of address strings and
    returns a list of tuples with the State, Suburb, and Postcode information.

    Parameters
    ----------
    addresses: list
        A list of address strings.
    Returns
    -------
    list
        A list of tuples with the State, Suburb, and Postcode information.
    Examples
    --------
    Example usage with an address:
    >>> addresses = ["10 OXFORD ST, EPPING, NSW 2121"]
    >>> parse_addresses(addresses)
    [('EPPING', 'NSW', '2121')]

    Explanation:
    The function splits the address string into individual components,
    assuming the comma (',') as the separator.
    It extracts the suburb from the second-to-last component
    (suburb_index = -2) in the address_parts list.
    The state and postcode are extracted from the last component
    (state_postcode_index = -1) in the address_parts list.
    The state is obtained from the first element
    (state_index = 0) in the state_postcode_parts list.
    The postcode is obtained from the last element
    (postcode_index = -1) in the state_postcode_parts list.
    The extracted information is returned
    as a tuple of (suburb, state, postcode).
    """

    parsed_addresses = []
    for address in addresses:
        address_parts = (address.strip().split(',')
                         if isinstance(address, str) else [])
        suburb_index = -2
        state_postcode_index = -1
        suburb = address_parts[suburb_index].strip() if len(
            address_parts) >= abs(suburb_index) else None

        state_postcode_parts = address_parts[state_postcode_index].strip().split(
            ' ') if len(address_parts) >= abs(state_postcode_index) else []
        state_index = 0
        postcode_index = -1
        state = state_postcode_parts[state_index] if state_postcode_parts else None
        postcode = state_postcode_parts[postcode_index] if len(
            state_postcode_parts) > 1 else None

        parsed_addresses.append((suburb, state, postcode))

    return parsed_addresses


def extract_first_last_name(licensee: str) -> pd.Series:
    """
    Extracts the first and last name
    from the licensee string and returns a Pandas Series.

    Parameters
    ----------
    licensee : str
        The licensee name string.

    Returns
    -------
    pandas.Series
        A Pandas series with
        the First Name and Last Name extracted from the licensee name string.
    Explanation
    -----------
    Licensee example: "Nannan Cheng"

    The index 0 is used to access the first name in the 'name_parts' list, resulting in 'Nannan':
    `first_name = name_parts[0]`

    The index -1 is not used in this case since there is only one name in the 'name_parts' list.
    Therefore, `last_name_index` is set to None.
    """
    # Check if licensee is a string
    if not isinstance(licensee, str) or not licensee:
        return pd.Series({'Last Name': None, 'First Name': None})

    name_parts = licensee.split(maxsplit=1)
    first_name_index = 0
    last_name_index = -1 if len(name_parts) > 1 else None

    first_name = name_parts[first_name_index]
    last_name = name_parts[last_name_index] if last_name_index is not None else None

    return pd.Series({'Last Name': last_name, 'First Name': first_name})


def merge_on_license_number(fairtrade_df, imis_df):
    """
    Merges two dataframes on the 'license_number' column.

    Parameters
    ----------
    fairtrade_df : pandas.DataFrame
        First dataframe to merge.
    imis_df : pandas.DataFrame
        Second dataframe to merge.

    Returns
    -------
    pandas.DataFrame
        Merged dataframe.
    """
    merging_on_list = ['license_number']
    suffixes = ['_fairtrade', '_reinsw']

    # Merge dataframes on license_number
    merge_on_license_number_df = pd.merge(
        fairtrade_df,
        imis_df,
        on=merging_on_list,
        suffixes=suffixes,
        how='inner')

    return merge_on_license_number_df


def merge_on_license_number_and_licensee(fairtrade_df, imis_df):
    """
    Merges two dataframes on the 'license_number' and licensee column.

    Parameters
    ----------
    fairtrade_df : pandas.DataFrame
        First dataframe to merge.
    imis_df : pandas.DataFrame
        Second dataframe to merge.

    Returns
    -------
    pandas.DataFrame
        Merged dataframe.
    """
    # fairtrade_df = fairtrade_df.drop_duplicates(
    #     subset=['license_number', 'licensee'])
    # imis_df = imis_df.drop_duplicates(subset=['license_number', 'licensee'])

    merging_on_list = ['license_number', 'licensee']
    suffixes = ['_fairtrade', '_reinsw']
    merge_on_license_number_and_licensee_df = pd.merge(
        fairtrade_df, imis_df, on=merging_on_list, suffixes=suffixes, how='inner')
    return merge_on_license_number_and_licensee_df


def merge_on_licence_number_licencee_fname_lname(fairtrade_df, imis_df):
    """
    Merges two dataframes on the 'first name' and 'last name' column.

    Parameters
    ----------
    fairtrade_df : pandas.DataFrame
        First dataframe to merge.
    imis_df : pandas.DataFrame
        Second dataframe to merge.

    Returns
    -------
    pandas.DataFrame
        Merged dataframe.
    """
    # fairtrade_df = fairtrade_df.drop_duplicates(
    #     subset=['first_name', 'last_name', 'licensee', 'license_number'])
    # imis_df = imis_df.drop_duplicates(
    #     subset=['first_name', 'last_name', 'licensee', 'license_number'])

    merging_on_list = ['first_name', 'last_name', 'license_number', 'licensee']
    suffixes = ['_fairtrade', '_reinsw']
    merge_on_fname_and_lname_df = pd.merge(
        fairtrade_df,
        imis_df,
        on=merging_on_list,
        suffixes=suffixes,
        how='inner')
    return merge_on_fname_and_lname_df


def merge_on_licence_number_licencee_fname_lname_address(
        fairtrade_df, imis_df):
    """
    Merges two dataframes on the 'post_code', 'suburb',
    and 'state' columns using iterator batching.

    Parameters
    ----------
    fairtrade_df : pandas.DataFrame
        First dataframe to merge.
    imis_df : pandas.DataFrame or iterator
        Second dataframe or iterator to merge.

    Returns
    -------
    pandas.DataFrame
        Merged dataframe.
    """

    merging_on_list = ['suburb', 'state', 'post_code',
                       'last_name', 'first_name', 'license_number', 'licensee']
    suffixes = ['_fairtrade', '_reinsw']

    fairtrade_df = fairtrade_df.drop_duplicates(subset=merging_on_list)
    fairtrade_df = fairtrade_df.dropna(subset=merging_on_list)

    results_merge_on_licence_number_licencee_fname_lname_address_df = []

    if isinstance(imis_df, pd.io.parsers.TextFileReader):
        imis_iterator = imis_df
    else:
        imis_iterator = [imis_df]

    for chunk_imis_df in imis_iterator:
        chunk_imis_df = chunk_imis_df.drop_duplicates(subset=merging_on_list)

        merged_reinsw_df = pd.merge(
            fairtrade_df,
            chunk_imis_df,
            on=merging_on_list,
            suffixes=suffixes,
            how='inner')

        filtered_merged_reinsw_df = merged_reinsw_df.dropna(
            subset=merging_on_list)
        filtered_merged_reinsw_df = filtered_merged_reinsw_df.drop_duplicates(
            subset=merging_on_list)

        results_merge_on_licence_number_licencee_fname_lname_address_df.append(
            filtered_merged_reinsw_df)

    merge_on_address_df = pd.concat(
        results_merge_on_licence_number_licencee_fname_lname_address_df)

    return merge_on_address_df


def merge_on_licencee(fairtrade_df, imis_df):
    """
    Merges two dataframes on the 'license_number' and licensee column.

    Parameters
    ----------
    fairtrade_df : pandas.DataFrame
        First dataframe to merge.
    imis_df : pandas.DataFrame
        Second dataframe to merge.

    Returns
    -------
    pandas.DataFrame
        Merged dataframe.
    """
    # fairtrade_df = fairtrade_df.drop_duplicates(
    #     subset=['licensee'])
    # imis_df = imis_df.drop_duplicates(subset=['licensee'])

    merging_on_list = ['licensee']
    suffixes = ['_fairtrade', '_reinsw']
    merge_on_licensee_df = pd.merge(fairtrade_df, imis_df, on=merging_on_list,
                                    suffixes=suffixes, how='inner')
    return merge_on_licensee_df


def merge_on_licencee_fname_lname(fairtrade_df, imis_df):
    """
    Parameters
    Merge two dataframes based on matching 'first_name', 'last_name', and 'licensee' columns.
    ----------
    fairtrade_df : pandas.DataFrame
        First dataframe to merge.
    imis_df : pandas.DataFrame
        Second dataframe to merge.

    Returns
    -------
    pandas.DataFrame
        Merged dataframe.
    """
    # fairtrade_df = fairtrade_df.drop_duplicates(
    #     subset=['first_name', 'last_name'])
    # imis_df = imis_df.drop_duplicates(subset=['first_name', 'last_name'])

    merging_on_list = ['first_name', 'last_name', 'licensee']
    suffixes = ['_fairtrade', '_reinsw']
    merge_on_licencee_fname_lname_df = pd.merge(fairtrade_df,
                                                imis_df,
                                                on=merging_on_list,
                                                suffixes=suffixes, how='inner')
    return merge_on_licencee_fname_lname_df


def merge_on_licencee_fname_lname_address(fairtrade_df, imis_df):
    """
    Merges two dataframes on the 'licencee', 'fname', 'lname'
    ,'post_code', 'suburb', and 'state' columns using iterator batching.

    Parameters
    ----------
    fairtrade_df : pandas.DataFrame
        First dataframe to merge.
    imis_df : pandas.DataFrame or iterator
        Second dataframe or iterator to merge.

    Returns
    -------
    pandas.DataFrame
        Merged dataframe.
    """

    merging_on_list = ['suburb', 'state', 'post_code',
                       'last_name', 'first_name', 'licensee']
    suffixes = ['_fairtrade', '_reinsw']

    fairtrade_df = fairtrade_df.drop_duplicates(subset=merging_on_list)
    fairtrade_df = fairtrade_df.dropna(subset=merging_on_list)

    results_merge_on_licencee_fname_lname_address_df = []

    if isinstance(imis_df, pd.io.parsers.TextFileReader):
        imis_iterator = imis_df
    else:
        imis_iterator = [imis_df]

    for chunk_imis_df in imis_iterator:
        chunk_imis_df = chunk_imis_df.drop_duplicates(subset=merging_on_list)

        merged_reinsw_df = pd.merge(
            fairtrade_df,
            chunk_imis_df,
            on=merging_on_list,
            suffixes=suffixes,
            how='inner')

        filtered_merged_reinsw_df = merged_reinsw_df.dropna(
            subset=merging_on_list)
        filtered_merged_reinsw_df = filtered_merged_reinsw_df.drop_duplicates(
            subset=merging_on_list)

        results_merge_on_licencee_fname_lname_address_df.append(
            filtered_merged_reinsw_df)

    merge_on_address_df = pd.concat(
        results_merge_on_licencee_fname_lname_address_df)

    return merge_on_address_df


def rename_columns(result_match_fair_reinsw_df):
    """
    Renames the columns in the merged dataframe
    according to a predefined mapping.

    Parameters
    ----------
    result_match_fair_reinsw : pandas.DataFrame
        Merged dataframe containing the matched Fairtrade and REINSW data.

    Returns
    -------
    pandas.DataFrame
        Merged dataframe with renamed columns.
    """
    columns_mapping = {
        "Licence Number": "license_number",
        "Licensee": "licensee",
        "First Name": "first_name",
        "Last Name": "last_name",
        "Postcode": "post_code",
        "Suburb": "suburb",
        "State": "state"
    }

    return result_match_fair_reinsw_df.rename(columns=columns_mapping)


def merge_columns_and_drop(result_df, column_mappings):
    """
    Merges columns in the DataFrame based on the provided mappings and drops the original columns.

    Parameters
    ----------
    result_df : pandas.DataFrame
        DataFrame to merge columns and drop original columns from.
    column_mappings : dict
        A dictionary containing the column mappings to merge and drop.
        The keys represent the new merged column names, and the values represent the list of columns to merge.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with merged columns and dropped original columns.
    """
    for new_column, columns_to_merge in column_mappings.items():
        valid_columns_to_merge = [
            col for col in columns_to_merge if col in result_df.columns
        ]

        # Initialize merged_values as an empty DataFrame
        merged_values = pd.DataFrame()

        # Handle merging of string columns
        if result_df[valid_columns_to_merge].dtypes.all() == str:
            merged_values = result_df[valid_columns_to_merge].replace(
                '', '').sum(axis=1)

        # Handle merging of numeric columns
        elif result_df[valid_columns_to_merge].dtypes.apply(pd.api.types.is_numeric_dtype).all():
            merged_values = result_df[valid_columns_to_merge].sum(axis=1)

        # Check if merged_values contains any non-empty values
        if not merged_values.empty and merged_values.astype(bool).any():
            result_df[new_column] = merged_values
            result_df = result_df.drop(columns=valid_columns_to_merge)

    return result_df


def merge_dataframes_with_mappings_and_columns(fairtrade_df,
                                               imis_df,
                                               merge_mappings,
                                               merge_columns_dict):
    """
    Merge dataframes based on predefined mappings and generate merged results.

    Parameters
    ----------
    fairtrade_df : pandas.DataFrame
        The first dataframe to merge.
    imis_df : pandas.DataFrame
        The second dataframe to merge.
    merge_mappings : dict
        A dictionary containing the merge mappings as keys and the merge functions as values.
    merge_columns_dict : dict
        A dictionary containing the merged column names as keys and the list of columns to merge as values.

    Returns
    -------
    dict
        A dictionary containing the merged dataframes for different merge mappings.
        The keys of the dictionary represent the merge mapping names, and the values
        are the corresponding merged dataframes.
    """
    results = {}

    for idx, (result_name, merge_func) in enumerate(merge_mappings.items(), 1):
        result_df = merge_func(fairtrade_df.copy(), imis_df)
        result_df = merge_columns_and_drop(result_df, merge_columns_dict)
        file_name = f"{idx}_{result_name}.csv"
        results[file_name] = result_df

    return results


def merge_cert_dataframes(certificate_df, reinsw_df):
    """
    Merge dataframes based on predefined mappings and generate merged results.

    Parameters
    ----------
    certificate_df : pandas.DataFrame
        The dataframe containing data on Fairtrade certificates/licenses.
    reinsw_df : pandas.DataFrame
        The dataframe containing data from the NSW government report.

    Returns
    -------
    dict
        A dictionary containing the merged dataframes for different merge mappings.
        The keys of the dictionary represent the merge mapping names, and the values
        are the corresponding merged dataframes.
    """
    merge_mappings = {
        'match_cert_and_imis_with_license_number': merge_on_license_number,
        'match_cert_and_imis_with_license_number_and_licensee': merge_on_license_number_and_licensee,
        'match_cert_and_imis_with_licence_number_licencee_fname_lname': merge_on_licence_number_licencee_fname_lname,
        'match_cert_and_imis_with_licence_number_licencee_fname_lname_address': merge_on_licence_number_licencee_fname_lname_address,
        'match_cert_and_imis_with_licensee': merge_on_licencee,
        'match_cert_and_imis_licencee_fname_lname': merge_on_licencee_fname_lname,
        'match_cert_and_imis_licencee_fname_lname_address': merge_on_licencee_fname_lname_address,
    }

    return merge_dataframes_with_mappings_and_columns(
        certificate_df,
        reinsw_df,
        merge_mappings,
        merge_columns_dict)


def merge_inv_dataframes(individual_df, reinsw_df):
    """
    Merge dataframes based on predefined mappings and generate merged results.

    Parameters
    ----------
    individual_df : pandas.DataFrame
        The dataframe containing data on Fairtrade certificates/licenses.
    reinsw_df : pandas.DataFrame
        The dataframe containing data from the NSW government report.

    Returns
    -------
    dict
        A dictionary containing the merged dataframes for different merge mappings.
        The keys of the dictionary represent the merge mapping names, and the values
        are the corresponding merged dataframes.
    """
    merge_mappings = {
        'match_inv_and_imis_with_license_number': merge_on_license_number,
        'match_inv_and_imis_with_license_number_and_license': merge_on_license_number_and_licensee,
        'match_inv_and_imis_with_licence_number_licencee_fname_lname': merge_on_licence_number_licencee_fname_lname,
        'match_inv_and_imis_licence_number_licencee_fname_lname_address': merge_on_licence_number_licencee_fname_lname_address,
        'match_inv_and_imis_with_licensee': merge_on_licencee,
        'match_inv_and_imis_with_licencee_fname_lname': merge_on_licencee_fname_lname,
        'match_inv_and_imis_licencee_fname_lname_address': merge_on_licencee_fname_lname_address,
    }
    return merge_dataframes_with_mappings_and_columns(
        individual_df,
        reinsw_df,
        merge_mappings,
        merge_columns_dict)


def preprocess_lname_fname_address_rename_column_df(fairtrading_df):
    """
    Preprocesses the Fairtrading dataframe by extracting the last name, first name,
    suburb, state, and postcode from the 'Licensee' and 'Address' columns,
    and renames the columns based on predefined mappings.

    Parameters
    ----------
    fairtrading_df : pandas.DataFrame
        The Fairtrading dataframe to be preprocessed.

    Returns
    -------
    pandas.DataFrame
        The preprocessed Fairtrading dataframe with the following modifications:
        - 'Last Name' and 'First Name' columns are extracted from the 'Licensee' column.
        - 'Suburb', 'State', and 'Postcode' columns are extracted from the 'Address' column.
        - Columns are renamed based on predefined mappings.

    Examples
    --------
    >>> df = pd.DataFrame({'Licensee': ['John Doe'], 'Address': ['10 Oxford St, Epping, NSW 2121']})
    >>> preprocess_lname_fname_address_rename_column_df(df)
       Last Name First Name  Suburb  State  Postcode
    0       Doe       John  Epping    NSW      2121
    """
    fairtrading_df[['Last Name', 'First Name']] = fairtrading_df['Licensee'].apply(
        extract_first_last_name)
    fairtrading_df[['Suburb', 'State', 'Postcode']
                   ] = parse_addresses(fairtrading_df['Address'])
    fairtrading_df = rename_columns(fairtrading_df)
    return fairtrading_df
