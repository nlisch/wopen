#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
import re
import io
import datetime
from glob import glob
from slackclient import SlackClient
import nest_asyncio
from df2gspread import gspread2df as g2d
from df2gspread import df2gspread as d2g
import gspread
from oauth2client.service_account import ServiceAccountCredentials


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
    table_id='association_raw'
)

# Get similarity data
query = read_sql('python_scripts/sql/get_associations_raw_data.sql', '', '', '')
print('Loading data from Query below : \n\n{}'.format(query))
similarity_data = execute_sql(query, dialect='standard')

#Get list asso to add from spreadsheet

# use creds to create a client to interact with the Google Drive API
scope = ['https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ['BIGQUERY_ACCESS_TOKEN_PATH'], scope)
client = gspread.authorize(creds)
spreadsheet_key = 'TO_REPLACE_WITH_GSHEET_TOKEN'
wks_name = 'assos_to_add'
to_add = g2d.download(spreadsheet_key, wks_name, credentials=creds, col_names = True, row_names = False)
print(to_add)
to_add = to_add['titre'].tolist()
assos_to_add = similarity_data.copy(deep=True)
assos_to_add = assos_to_add[assos_to_add['titre'].isin(to_add)]
print(assos_to_add)

#Get association_already_validated from BQ and filter out them from data to process
query = read_sql('python_scripts/sql/get_associations_validation_data.sql', '', '', '')
print('Loading data from Query below : \n\n{}'.format(query))
association_already_validated = execute_sql(query, dialect='standard')
association_already_validated['titre'] = association_already_validated['titre'].str.upper() 
list_association_already_validated = association_already_validated['titre'].tolist()
print('Assos already validated {}'.format(list_association_already_validated))
print(similarity_data['titre'])
similarity_data = similarity_data[~similarity_data['titre'].isin(list_association_already_validated)]
assos_to_add = assos_to_add[~assos_to_add['titre'].isin(list_association_already_validated)]

#Filter on FB url searched, with potential FB accounts
similarity_data_filtered = similarity_data[~similarity_data['Facebook_ID'].isnull()]
similarity_data_filtered = similarity_data_filtered[similarity_data_filtered['check_levenshtein_similarity_facebook'] != 'no_fb_account']
similarity_data_filtered = similarity_data_filtered.append(assos_to_add, ignore_index=True, sort=False)

similarity_data_filtered.loc[similarity_data_filtered.adrg_codepostal.str.contains('^75|PARIS', regex= True, na=False), 'adrg_achemine'] = 'Paris'
similarity_data_filtered['combined_address'] = similarity_data_filtered['adrg_libvoie'].astype(str) + ', '+ similarity_data_filtered['adrg_codepostal'].astype(str)  + ', '+ similarity_data_filtered['adrg_achemine'].astype(str)
similarity_data_filtered['combined_address'] = similarity_data_filtered['combined_address'].str.title()

#Select columns to send to spreadsheet
similarity_data_filtered['fb_validation'] = ''
similarity_data_filtered.loc[similarity_data_filtered['check_levenshtein_similarity_facebook'] == 'fb_account', 'fb_validation'] = 'yes'
similarity_data_filtered['main_category'] = ''
similarity_data_filtered['main_type'] = ''
similarity_data_filtered = similarity_data_filtered[['titre','Facebook_new_URL','fb_validation','combined_address', 'main_category', 'main_type', 'Objet_Social', 'objet','date_extract','adrs_codepostal','Facebook_ID']]
similarity_data_filtered = similarity_data_filtered.reset_index()
similarity_data_filtered = similarity_data_filtered.sort_values(by=['date_extract','titre'], ascending=True)
similarity_data_filtered = similarity_data_filtered[~similarity_data_filtered.titre.str.contains('LYCEE|FESTIVAL|COLLEGE|PRODUCTION', regex= True, na=False)]


# use creds to create a client to interact with the Google Drive API
scope = ['https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ['BIGQUERY_ACCESS_TOKEN_PATH'], scope)
client = gspread.authorize(creds)

spreadsheet_key = 'TO_REPLACE_WITH_GSHEET_TOKEN'
wks_name = 'fb_check_upload'
print(similarity_data_filtered.shape)
d2g.upload(similarity_data_filtered, spreadsheet_key, wks_name, credentials=creds, row_names=True, clean=True)
print("Similarity data updated to Spreadsheet")

slack_client.api_call(
  "chat.postMessage",
  channel=slack_channel,
  text='Similarity data updated to Spreadsheet'
)

