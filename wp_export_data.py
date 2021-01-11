# -*- coding: utf-8 -*-

import pandas as pd
import requests
import os
import re
import io
import locale
import numpy as np
locale.setlocale(locale.LC_TIME,'')
from slackclient import SlackClient
import nest_asyncio
import datetime

#Local function
from lib.class_bigquery import BigqueryTable

from dotenv import load_dotenv
load_dotenv()
print("Packages imported")


# Get users
url="TO_REPLACE_WP_ID_JOB_IMPORT"
s=requests.get(url).content
users = pd.read_csv(io.StringIO(s.decode('utf-8')), error_bad_lines=False, sep=';', encoding="utf-8")
users.rename(columns = {'id':'Author_ID'}, inplace = True)

# Get Associations
url="TO_REPLACE_WP_ID_JOB_IMPORT"
s=requests.get(url).content
listings = pd.read_csv(io.StringIO(s.decode('utf-8')), error_bad_lines=False, sep=';', encoding="utf-8")
listings['Categories'].loc[listings.Categories.isnull()] = 'No category specifed'
print(listings.columns)

# Aggregate listings
listings_grouped = listings.copy(deep=True)
listings_grouped = listings_grouped.loc[listings_grouped._listing_type == 'service']
listings_grouped = listings.groupby(['Author ID'], as_index=False).agg({'Title': 'count'})
listings_grouped.rename(columns = {'Title':'nb_listing_owned'}, inplace = True)
listings_grouped.rename(columns = {'Author ID':'Author_ID'}, inplace = True) 


data_clean = pd.merge(users, listings_grouped,
                       how='left', on=['Author_ID'])
data_clean['nb_listing_owned'].loc[data_clean.nb_listing_owned.isnull()] = 0
data_clean['listing_owned'] = pd.Series()
data_clean['listing_owned'].loc[data_clean['nb_listing_owned'] > 0 ] = 'owned_listing'
data_clean['listing_owned'].loc[data_clean['nb_listing_owned'] == 0 ] = 'no_owned_listing'

#Format correctly columns users table
data_clean.columns = data_clean.columns.str.replace(u"é", "e")
data_clean.columns = data_clean.columns.str.replace(' ', '_')
data_clean.columns = data_clean.columns.str.replace('è', 'e')
data_clean.columns = data_clean.columns.str.replace('à', 'a')
data_clean.columns = data_clean.columns.str.replace("'", "_")
data_clean.columns = data_clean.columns.str.replace("-", "_")

#Format correctly columns associations table 
listings.columns = listings.columns.str.replace(u"é", "e")
listings.columns = listings.columns.str.replace(' ', '_')
listings.columns = listings.columns.str.replace('è', 'e')
listings.columns = listings.columns.str.replace('à', 'a')
listings.columns = listings.columns.str.replace("'", "_")
listings.columns = listings.columns.str.replace("-", "_")

# Initialise the users table
table = BigqueryTable(
    dataset_id='crm',
    table_id='wp_export_users'
)

#Write to BigQuery
table.write(
     df=data_clean
)
print ('Users table loaded in BigQuery')

# Initialise the users table
table = BigqueryTable(
    dataset_id='crm',
    table_id='wp_export_associations'
)

#Write to BigQuery
table.write(
     df=listings
)
print ('Association table loaded in BigQuery')

# Slack alert
slack_client = SlackClient(token=os.environ['SLACK_API_TOKEN'])
slack_message = 'Users and Associations export tables updated in BigQuery'
slack_channel = '#script_automation'

nest_asyncio.apply()

slack_client.api_call(
  "chat.postMessage",
  channel=slack_channel,
  text=slack_message
)
print('Update sent to Slack #script_automation')

