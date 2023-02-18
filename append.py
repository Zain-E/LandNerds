# Press ⌃R to execute it or replace it with your code.
# Press shift, option, E to execute highlighted code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

#-----------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------

import pandas as pd
import pandas_gbq as pd_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from google.cloud import storage

# Instantiates a client to GCS
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'landnerds-cloud-storage.json'
client = storage.Client()
bucket_name = 'landnerds/backup_daily_scrape'
# Set the name of the BigQuery dataset and table you want to read
dataset_name = 'landnerds.propertysourcer_dailyhouseprices'

# Set up your GCP project and service account credentials (downloaded as a JSON from 'service accounts' on GCP)
credentials = service_account.Credentials.from_service_account_file('landnerds-gbq.json')
project_id = 'landnerds'

# Connect to the right project id and client
client = bigquery.Client(credentials=credentials,project=project_id)

# We want to loop through certain tables, therefore we need to access the names of these tables
# Using the information schema function on SQL i have downloaded all table info into an excel sheet
df_schema = pd.read_excel('information_schema.xlsx')

# We only want tables up until 20200919, beyond this the data types are correct
year = 2023
tables = df_schema['table_name'].loc[(df_schema['table_name'] > 20221231) & (df_schema['table_name'] <= 20231231)]
tables = tables.values.tolist()
print(tables)
print(len(tables))

# Read the GBQ table into a df and add column names
df = pd_gbq.read_gbq(f'{dataset_name}.{tables[0]}', project_id=f'{project_id}')
df['date'] = ''
df.head(0).to_csv(f'/Users/zaineisa/Desktop/{year}.csv', index=False)

# Loop through the list of tables
for table in tables:

    # Read the GBQ table into a df
    df_loop = pd_gbq.read_gbq(f'{dataset_name}.{table}', project_id=f'{project_id}')

    # Add a new column with the date and append
    df_loop['date'] = table

    # Save locally as csv
    df_local = df_loop.to_csv(f'/Users/zaineisa/Desktop/{year}.csv', mode='a', header=False, index=False)

# Save to csv on GCS
df = pd.read_csv(f'/Users/zaineisa/Desktop/{year}.csv')
df.to_csv(f'gs://{bucket_name}/{year}.csv',mode='a',header= False, index=False)

# Print df and describe to double check..
df = pd.read_csv(f'gs://{bucket_name}/{year}.csv')
print(f'table shape is {df.shape}. It has been successfully uploaded to gcs.')

# Delete file locally
os.remove(f'/Users/zaineisa/Desktop/{year}.csv')
