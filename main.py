# Press ⌃R to execute it or replace it with your code.
# Press shift, option, E to execute highlighted code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


#------------------------------------     UPDATE (GBQ CONNECT)     -----------------------------------------
#-----------------------------------------------------------------------------------------------------------

import pandas as pd
import pandas_gbq as pd_gbq
from google.cloud import bigquery
from google.oauth2 import service_account

# Set up your GCP project and service account credentials (downloaded as a JSON from 'service accounts' on GCP)
credentials = service_account.Credentials.from_service_account_file('landnerds-gbq.json')
project_id = 'landnerds'

# Connect to the right project id and client
client = bigquery.Client(credentials=credentials,project=project_id)

# Set the name of the BigQuery dataset and table you want to read
dataset_name = 'landnerds.propertysourcer_dailyhouseprices'

# We want to loop through certain tables, therefore we need to access the names of these tables
# Using the information schema function on SQL i have downloaded all table info into an excel sheet
df_schema = pd.read_excel('information_schema.xlsx')

# We only want tables up until 20200919, beyond this the data types are correct
tables = df_schema['table_name'].loc[(df_schema['table_name'] <= 20200919)]
tables = tables.values.tolist()
print(tables)
print(len(tables))

# Loop through the list of tables
for table in tables:

    # Read the GBQ table into a df
    df = pd_gbq.read_gbq(f'{dataset_name}.{table}', project_id=f'{project_id}')

    # Transform the df using pandas - i.e. fix DQ issues, specifically remove "" from the fields
    df['num_beds'] = df['num_beds'].replace('""','')
    df['num_recepts'] = df['num_recepts'].replace('""','')
    df['num_baths'] = df['num_baths'].replace('""','')
    df['acorn_type'] = df['acorn_type'].replace('""','')
    # df['price'] = df['price'].replace('??','??') - can't remember what DQ issue I saw with price!

    # Write the df back to Google Big Query as either a new table or override a pre-existing table
    df.to_gbq(f'{dataset_name}.{table}',f'{project_id}',if_exists='replace')

# Print df to double check..
print(str(len(tables))+' tables successfully altered.')


#------------------------------     BACKUP (GOOGLE CLOUD STORAGE)    ---------------------------------------
#-----------------------------------------------------------------------------------------------------------

import pandas as pd
import pandas_gbq as pd_gbq
from google.cloud import bigquery
from google.oauth2 import service_account

# Set up your GCP project and service account credentials (downloaded as a JSON from 'service accounts' on GCP)
credentials = service_account.Credentials.from_service_account_file('landnerds-gbq.json')
project_id = 'landnerds'

# Connect to the right project id and client
client = bigquery.Client(credentials=credentials,project=project_id)

# Set the name of the BigQuery dataset and table you want to read
dataset_name = 'landnerds.propertysourcer_dailyhouseprices'

# We want to loop through certain tables, therefore we need to access the names of these tables
# Using the information schema function on SQL i have downloaded all table info into an excel sheet
df_schema = pd.read_excel('information_schema.xlsx')
df_remain = pd.read_excel('remaining_tables.xlsx')

# We only want tables up until 20200919, beyond this the data types are correct
tables = df_schema['table_name'].loc[(df_schema['table_name'] > 20200919)]
tables = tables.values.tolist()
print(tables)
print(len(tables))

# Imports the Google Cloud client library
import os
from google.cloud import storage

# Instantiates a client
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'landnerds-cloud-storage.json'
client = storage.Client()
bucket_name = 'landnerds/backup_daily_scraper'

# Loop through the list of tables
for table in tables:

    # Read the GBQ table into a df
    df = pd_gbq.read_gbq(f'{dataset_name}.{table}', project_id=f'{project_id}')

    # Save the df to google cloud storage as is
    bucket = client.bucket(f'{bucket_name}')
    blob = bucket.blob(f'{table}.csv')
    # needs gcsfs package to run the below (save to gcp with a simple filepath)
    upload = df.to_csv(f'gs://{bucket_name}/{table}.csv')

    # Print df to double check..
print(str(len(tables))+f' tables successfully saved to GCP bucket {bucket_name}.')
