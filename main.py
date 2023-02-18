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


#--------------------------------     BACKUP (GOOGLE CLOUD STORAGE)    -------------------------------------
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

#-----------------------------  UNIFY + BACKUP (GOOGLE CLOUD STORAGE) --------------------------------------
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
year = 2020
tables = df_schema['table_name'].loc[(df_schema['table_name'] > 20191231) & (df_schema['table_name'] <= 20201231)]
tables = tables.values.tolist()
print(tables)
print(len(tables))

# Read the GBQ table into a df and add column names
df = pd_gbq.read_gbq(f'{dataset_name}.{tables[0]}', project_id=f'{project_id}')
df['date'] = ''


# Reorder the column names of df to ensure alignment
if 'listing_id' not in df:
    df['listing_id']=''

df= df[['acorn_type', 'property_type', 'num_baths', 'num_recepts', 'num_beds', 'postcode', 'postcode_district', 'outcode',
     'display_address', 'listing_id', 'listing_condition', 'url', 'price', 'date']]

df.head(0).to_csv(f'/Users/zaineisa/Desktop/{year}.csv', index=False)

# Rename the columns in accordance with the reordered columns
# df = df.rename(columns={"property_type": "acorn_type", "num_baths": "property_type", "postcode_district": "num_baths","num_recepts": "num_recepts", "num_beds": "num_beds", "postcode": "postcode","outcode": "postcode_district", "acorn_type": "outcode", "display_address": "display_address","price": "listing_id", "listing_id": "listing_condition", "url": "url","listing_condition": "price", "date": "date"}, inplace=True)


# Loop through the list of tables
for table in tables:

    # Read the GBQ table into a df
    df_loop = pd_gbq.read_gbq(f'{dataset_name}.{table}', project_id=f'{project_id}')

    # Add a new column with the date and append
    df_loop['date'] = table

    # Reorder the columns of df_loop to ensure alignment
    # Please note, we have to create if statements to create df columns where none exist
    if 'listing_id' not in df_loop:
        df_loop['listing_id'] = ''
    if 'num_baths' not in df_loop:
        df_loop['num_baths'] = ''
    if 'num_recepts' not in df_loop:
        df_loop['num_recepts'] = ''
    if 'num_beds' not in df_loop:
        df_loop['num_beds'] = ''
    if 'acorn_type' not in df_loop:
        df_loop['acorn_type'] =''
    if 'display_address' not in df_loop:
        df_loop['display_address'] =''

    df_loop = df_loop[['acorn_type','property_type','num_baths','num_recepts','num_beds','postcode','postcode_district','outcode','display_address','listing_id','listing_condition','url','price','date']]

    # Save locally as csv - appending one table onto another
    df_local = df_loop.to_csv(f'/Users/zaineisa/Desktop/{year}.csv', mode='a', header=False, index=False)

# Data quality issues need to be resolved (i.e. strings within the price field)

# Save to csv on GCS
df = pd.read_csv(f'/Users/zaineisa/Desktop/{year}.csv')
df.to_csv(f'gs://{bucket_name}/{year}.csv',mode='a',header= False, index=False)

# Print df and describe to double check..
df = pd.read_csv(f'gs://{bucket_name}/{year}.csv')
print(f'table shape is {df.shape}. It has been successfully uploaded to gcs.')

# Delete file locally
os.remove(f'/Users/zaineisa/Desktop/{year}.csv')


