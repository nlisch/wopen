#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import requests
import zipfile
import os
import advertools as adv
import re
import io
import datetime
from glob import glob
import random
from slackclient import SlackClient
import nest_asyncio
from fuzzywuzzy import process, fuzz
import numpy as np
from datetime import timedelta, date

#Local function
from lib.class_bigquery import BigqueryTable
from lib.google_cloud_storage_functions import store_data_to_google_cloud_storage
from lib.bigquery_functions import execute_sql, read_sql

# settings.py
from dotenv import load_dotenv
load_dotenv()
print("Packages imported")

# Slack initialisation
slack_client = SlackClient(token=os.environ['SLACK_API_TOKEN'])
slack_channel = '#script_automation'

nest_asyncio.apply()

# Initialise the similarity table
table = BigqueryTable(
    dataset_id='crm',
    table_id='similarity'
)

table.create_from_schema_partition_table(schema_path='schema/similarity.json',
                                         partition_field='date_extract')

def get_fbid(fb_url):
    URL = "https://findmyfbid.com/"
    PARAMS = {'url': fb_url}
    try:
        r = requests.post(url = URL, params= PARAMS)
        return r.json().get("id")
    except Exception:
        return 0

def get_URL(fb_id):
    URL = "https://facebook.com/{}".format(fb_id)
    try:
        r = requests.get(URL, allow_redirects=True)
        r.status_code  # 302
        return r.url
    except Exception:
        return 0

"""def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

# Define dates to iterate
list_dates = []

start_dt = date(2020, 4, 6)
end_dt = datetime.date.today()
for dt in daterange(start_dt, end_dt):
    list_dates.append(dt.strftime("%Y%m%d"))
print('Dates list {}'.format(list_dates))"""

#Get partitions dates for similarity table
query = read_sql('python_scripts/sql/get_partitions_similarity.sql', '', '', '')
print('Loading data from Query below : \n\n{}'.format(query))
partitions_dates_associations_raw = execute_sql(query, dialect='legacy')
list_partitions_dates = partitions_dates_associations_raw['partition_id'].tolist()
print('Partitions Dates list similarity table {}'.format(list_partitions_dates))

#Get partitions dates for cusotm search table
query = read_sql('python_scripts/sql/get_partitions_custom_search.sql', '', '', '')
print('Loading data from Query below : \n\n{}'.format(query))
partitions_dates_custom_search = execute_sql(query, dialect='legacy')
list_partitions_dates_custom_search = partitions_dates_custom_search['partition_id'].tolist()
print('Partitions Dates list custom search {}'.format(list_partitions_dates_custom_search))

for date_string in list_partitions_dates_custom_search:

  # Check if date's file has already processed
  if date_string in list_partitions_dates:
    today_string = datetime.datetime.today().strftime('%Y%m%d')
    if date_string == today_string:
      print("Similarity data already processed today")
      slack_client.api_call(
      "chat.postMessage",
      channel=slack_channel,
      text='Similarity data already processed today'
    )
    else:
      print('Similarity data already processed for {}'.format(date_string))
  else:

    print ('Start getting similarity for {}'.format(date_string))

    
    # Get Custom search data for specific date
    query = read_sql('python_scripts/sql/get_date_custom_search_data.sql',date_string, '', '')
    print('Loading data from Query below : \n\n{}'.format(query))
    custom_search_data = execute_sql(query, dialect='standard')
    print(custom_search_data)

    # Filter on first search result
    custom_search_data = custom_search_data.loc[custom_search_data['rank'] == 1]
    custom_search_data['titre'] = custom_search_data['searchTerms'].copy(deep=True)
    custom_search_data['titre'] = custom_search_data['titre'].str.upper()

    #Get FB ID
    print("Start getting FB ID..")
    custom_search_data["Facebook_ID"] = custom_search_data.link.apply(get_fbid)
    custom_search_data["Facebook_ID"] = custom_search_data["Facebook_ID"].astype(str)
    custom_search_data["Facebook_URL"] = custom_search_data.Facebook_ID.apply(get_URL)
    custom_search_data['link_new'] = custom_search_data["Facebook_URL"].copy(deep=True)
    custom_search_data['link_new'] = np.where((custom_search_data['Facebook_ID'] == '0'), custom_search_data.link, custom_search_data.link_new)
    custom_search_data["Facebook_new_URL"] = custom_search_data['link_new'].copy(deep=True)


    # Preprocess custom search before checking similarities
    custom_search_data = custom_search_data[['titre','searchTerms','link', 'link_new', 'Facebook_ID', 'Facebook_URL','Facebook_new_URL','date_extract']]
    custom_search_data['link_new'] = custom_search_data['link_new'].str.extract('([^/.*/]+)/?$', expand=True)
    custom_search_data['link_new'] = custom_search_data['link_new'].str.lower()
    custom_search_data['link_new'] = custom_search_data['link_new'].str.strip()
    custom_search_data['link_new'] = custom_search_data['link_new'].str.replace(" ","")
    custom_search_data['link_new'] = custom_search_data['link_new'].str.replace("%C3%A9","e")
    custom_search_data['link_new'] = custom_search_data['link_new'].str.replace("%c%a","e")
    custom_search_data['link_new'] = custom_search_data['link_new'].str.replace('\d+', '')
    custom_search_data['link_new'] = custom_search_data['link_new'].str.replace('-', '')
    custom_search_data['searchTerms'] = custom_search_data['searchTerms'].str.lower()
    custom_search_data['searchTerms'] = custom_search_data['searchTerms'].str.strip()
    custom_search_data['searchTerms'] = custom_search_data['searchTerms'].str.replace(" ","")
    custom_search_data['date_extract'] = pd.to_datetime(custom_search_data['date_extract'], format='%Y%m%d')
    custom_search_data['date_extract'] = custom_search_data['date_extract'].dt.date

    
    print('Start Levenshtein Similarity Calculation')
    # Get levenshtein similarity
    levenshtein_similarity = []
    for i in custom_search_data.searchTerms:
            ratio = process.extract( i, custom_search_data.link_new, limit=1)
            levenshtein_similarity.append(ratio[0][1])
    custom_search_data['levenshtein_similarity'] = levenshtein_similarity

    print ('Potentials candidates for FB Page : {:,.0f}'.format(custom_search_data[custom_search_data['levenshtein_similarity'] > 60].shape[0]))

    table.write_partition_table(partition_date=date_string,
                      partition_field='date_extract',
                      data=custom_search_data,
                      schema_path='schema/similarity.json')


# Get data from similarity table
query = read_sql('python_scripts/sql/get_similarity_data.sql', '', '', '')
print('Loading data from Query below : \n\n{}'.format(query))
similarity_data = execute_sql(query, dialect='standard')

# Get RNA data filtered
# Initialise the similarity table
table = BigqueryTable(
    dataset_id='crm',
    table_id='data_rna_waldec_filtered'
)

#Get RNA data filtered from bigquery table
query = read_sql('python_scripts/sql/get_rna_waldec_filtered_data.sql', '', '', '')
print('Loading data from Query below : \n\n{}'.format(query))
data_rna_waldec_filtered = execute_sql(query, dialect='standard')

data_final = pd.merge(data_rna_waldec_filtered, similarity_data,
                       how='left', on=['titre'])

data_final.columns = data_final.columns.str.replace(' ', '_')

# Add check FB levenshtein similarity
data_final['check_levenshtein_similarity_facebook'] = data_final['levenshtein_similarity']
data_final.loc[data_final['levenshtein_similarity'] == 100, 'check_levenshtein_similarity_facebook'] = 'fb_account'
data_final.loc[(data_final['levenshtein_similarity'] >= 70) & (data_final['levenshtein_similarity'] < 100), 'check_levenshtein_similarity_facebook'] = 'potential_fb_account'
data_final.loc[(data_final['levenshtein_similarity'] < 70) | (data_final['levenshtein_similarity'].isnull()), 'check_levenshtein_similarity_facebook'] = 'no_fb_account'

#Clean column type
data_final['adrs_codepostal'] = data_final['adrs_codepostal'].astype(str)

print ('Check FB levenshtein similarity added')

# Initialise the association raw table
table = BigqueryTable(
    dataset_id='crm',
    table_id='associations_raw'
)

#Write to BigQuery
table.write(
     df=data_final
)

# Slack alert

slack_client.api_call(
  "chat.postMessage",
  channel=slack_channel,
  text='FB urls file updated to Cloud Storage & BigQuery'
)
print('Update to GS and BQ sent to Slack #script_automation')
