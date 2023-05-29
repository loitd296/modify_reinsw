import os

CERTIFICATE_ORIGINAL_PATH = "data/certificate.csv"
INDIVIDUAL_ORIGINAL_PATH = "data/individual.csv"
CERTIFICATE_PATH = "data/certificates_with_status.csv"
INDIVIDUAL_PATH = "data/individuals_with_status.csv"
REINSW_PATH = "data/reinsw_report.csv"

OUTPUT_PATH = "data/output.csv"
OUTPUT_PATH_CERTIFICATE = "data/licence_certificate.csv"
OUTPUT_PATH_INDIVIDUAL = "data/licence_individual.csv"
OUTPUT_PATH_REINSW = "data/licence_reinsw.csv"

# matching certificate & individual
CERTIFICATE_LICNUM = 'result-of-IMIS-with-fairtrading-and-certificates/match_cert_and_imis_with_license_number.csv'
CERTIFICATE_LICNUM_LIC = 'result-of-IMIS-with-fairtrading-and-certificates/match_cert_and_imis_with_license_number_and_licensee.csv'
CERTIFICATE_LICNUM_LIC_FNAME_LNAME = 'result-of-IMIS-with-fairtrading-and-certificates/match_cert_and_imis_with_licence_number_licencee_fname_lname.csv'
CERTIFICATE_FULL_CONDITION = 'result-of-IMIS-with-fairtrading-and-certificates/match_cert_and_imis_with_licence_number_licencee_fname_lname_address.csv'
CERTIFICATE_LIC = 'result-of-IMIS-with-fairtrading-and-certificates/match_cert_and_imis_with_licensee.csv'
CERTIFICATE_LIC_FNAME_LNAME = 'result-of-IMIS-with-fairtrading-and-certificates/match_cert_and_imis_licencee_fname_lname.csv'
CERTIFICATE_LIC_FNAME_LNAME_ADDRESS = 'result-of-IMIS-with-fairtrading-and-certificates/match_cert_and_imis_licencee_fname_lname_address.csv'

INDIVIDUAL_LICNUM = 'result-of-IMIS-with-fairtrading-and-certificates/match_inv_and_imis_with_license_number.csv'
INDIVIDUAL_LICNUM_LIC = 'result-of-IMIS-with-fairtrading-and-certificates/match_inv_and_imis_with_license_number_and_license.csv'
INDIVIDUAL_LICNUM_LIC_FNAME_LNAME = 'result-of-IMIS-with-fairtrading-and-certificates/match_inv_and_imis_with_licence_number_licencee_fname_lname.csv'
INDIVIDUAL_FULL_CONDITION = 'result-of-IMIS-with-fairtrading-and-certificates/match_inv_and_imis_licence_number_licencee_fname_lname_address.csv'
INDIVIDUAL_LIC = 'result-of-IMIS-with-fairtrading-and-certificates/match_inv_and_imis_with_licensee.csv'
INDIVIDUAL_LIC_FNAME_LNAME = 'result-of-IMIS-with-fairtrading-and-certificates/match_inv_and_imis_with_licencee_fname_lname.csv'
INDIVIDUAL_LIC_FNAME_LNAME_ADDRESS = 'result-of-IMIS-with-fairtrading-and-certificates/match_inv_and_imis_licencee_fname_lname_address.csv'

# NSW
AUTHORIZATION = os.getenv(
    "AUTHORIZATION",
    "Basic ZWpwR0FjcUUyOTI0VmlJRUd3NlJLcnVpUmFJczA5UkE6RDhidjJyREpsemhlVE9vQQ==")
API_KEY = os.getenv("API_KEY", "ejpGAcqE2924ViIEGw6RKruiRaIs09RA")
ACCESS_TOKEN_URL = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken?grant_type=client_credentials"
VERIFICATION_URL = "https://api.onegov.nsw.gov.au/propertyregister/v1/verify?licenceNumber="
BROWSE_URL = "https://api.onegov.nsw.gov.au/propertyregister/v1/browse?searchText="
DETAIL_URL = "https://api.onegov.nsw.gov.au//propertyregister/v1/details?licenceID="

# CSV_HEADER = ["company", "licensee", "imis_id", "state", "suburb",
#               "license_is_valid", "license_date", "license_number",
#               "first_name", "last_name", "post_code", "created_at", "updated_at",
#               "licence_status", "licence_type", "licence_id", "classes",
#               "class_names", "history", "expiring", "new", "is_change",]

CSV_HEADER = ["licensee", "state", "suburb", "license_is_valid", "license_date",
              "license_number", "first_name", "last_name", "post_code", "created_at", "updated_at",
              "licence_status", "licence_type", "licence_id", "classes", "class_names", "history", "expiring"]
