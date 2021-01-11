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
from geopy.geocoders import Nominatim

#Local function
from lib.class_bigquery import BigqueryTable

# settings.py
from dotenv import load_dotenv
load_dotenv()
print("Packages imported")

# Slack initialisation
slack_client = SlackClient(token=os.environ['SLACK_API_TOKEN'])
slack_channel = '#script_automation'

nest_asyncio.apply()

def get_coordinates(address):
    geolocator = Nominatim(user_agent="test", timeout=3)
    location = geolocator.geocode(address)
    if location:
        coordinates = str(location.latitude) + ',' + str(location.longitude)
    else:
        coordinates = ''
    return coordinates

# Initialise the association table
table = BigqueryTable(
    dataset_id='crm',
    table_id='associations_validation'
)

# use creds to create a client to interact with the Google Drive API
scope = ['https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ['BIGQUERY_ACCESS_TOKEN_PATH'], scope)
client = gspread.authorize(creds)

spreadsheet_key = 'TO_REPLACE_WITH_GSHEET_TOKEN'
wks_name = 'fb_check_upload'
fb_check = g2d.download(spreadsheet_key, wks_name, credentials=creds, col_names = True, row_names = True)
print("FB fb_check_upload")

# Save results as today's file
today_string = datetime.datetime.today().strftime('%Y%m%d')
fb_check['date_upload'] = today_string
fb_check['date_upload'] = pd.to_datetime(fb_check['date_upload'], format='%Y%m%d')
fb_check['date_upload'] = fb_check['date_upload'].dt.date
fb_check['date_extract'] = fb_check['date_extract'].str[:10]
fb_check['date_extract'] = pd.to_datetime(fb_check['date_extract'], format='%Y-%m-%d')
fb_check['date_extract'] = fb_check['date_extract'].dt.date
fb_check['adrs_codepostal'] = fb_check['adrs_codepostal'].str[:5]

fb_check = fb_check.drop(['index'], axis=1)

#Filter on assos checked
fb_check = fb_check[(fb_check['main_category'] != '') | (fb_check['fb_validation'] == 'no')]
print(fb_check)
print(fb_check.columns)
print(fb_check.dtypes)
print(fb_check)

#Format size police
fb_check['titre'] = fb_check['titre'].str.title()
fb_check['objet'] = fb_check['objet'].str.capitalize()

fb_check_to_save = fb_check[['titre', 'fb_validation', 'main_category', 'main_type', 'Facebook_new_URL', 'Objet_Social', 'combined_address', 'date_upload', 'date_extract', 'objet', 'adrs_codepostal']]

table.create_from_schema_partition_table(schema_path='schema/associations_validation.json',
                                         partition_field='date_upload')

table.write_partition_table(partition_date=today_string,
                  			partition_field='date_upload',
                 			data=fb_check_to_save,
                  			schema_path='schema/associations_validation.json')

#Filter validated association
fb_check = fb_check[fb_check['main_category'] != '']

#Get coordinates
fb_check['coordinates'] = fb_check['combined_address'].apply(get_coordinates)
fb_check = pd.concat([fb_check, fb_check['coordinates'].str.split(',', expand=True)], axis=1)
fb_check.rename(columns = {0:'Lattitude', 1:'Longitude'}, inplace = True) 

wks_name = 'association_validated'
asso_validated_past = g2d.download(spreadsheet_key, wks_name, credentials=creds, col_names = True, row_names = True)

fb_check = asso_validated_past.append(fb_check, ignore_index=True, sort=False)

print(fb_check)

# Remove dupplicated assos
fb_check = fb_check.drop_duplicates(subset=["titre"], keep='last')

fb_check['arrondissement'] = fb_check['adrs_codepostal'].astype(int)
print(fb_check['adrs_codepostal'])
print(fb_check['arrondissement'])

dict_to_use = {
 75001: "Paris 01",
 75002: "Paris 02",
 75003: "Paris 03",
 75004: "Paris 04",
 75005: "Paris 05",
 75006: "Paris 06",
 75007: "Paris 07",
 75008: "Paris 08",
 75009: "Paris 09",
 75010: "Paris 10",
 75011: "Paris 11",
 75012: "Paris 12",
 75013: "Paris 13",
 75014: "Paris 14",
 75015: "Paris 15",
 75016: "Paris 16",
 75017: "Paris 17",
 75018: "Paris 18",
 75019: "Paris 19",
 75020: "Paris 20"

}
fb_check['arrondissement'] = fb_check['arrondissement'].map(dict_to_use)
print(fb_check['arrondissement'])

#Filter out when no mapping arrondissement ( wrong postal code format)
fb_check = fb_check[fb_check.arrondissement.str.contains('^Paris', regex= True, na=False)]

#Order columns
fb_check = fb_check[['titre', 'main_category', 'main_type', 'Facebook_new_URL', 'Objet_Social', 'combined_address', 'date_upload', 'date_extract', 'objet', 'coordinates', 'Lattitude', 'Longitude','adrs_codepostal','arrondissement', 'Facebook_ID']]

#Assos to remove
wks_name = 'assos_to_remove'
to_remove = g2d.download(spreadsheet_key, wks_name, credentials=creds, col_names = True, row_names = False)
to_remove['titre'] = to_remove['titre'].str.title()
asso_to_remove = to_remove['titre'].tolist()
fb_check = fb_check[~fb_check['titre'].isin(asso_to_remove)]

wks_name = 'association_validated'										
d2g.upload(fb_check, spreadsheet_key, wks_name, credentials=creds, row_names=True, clean=True)

print('Validated associations updated')

fb_check.to_csv('~/public/wp-content/uploads/wpallimport/files/assos_to_create.csv',encoding='utf-8', sep=';')
print('Validated assos file ready for WP import')

slack_client.api_call(
  "chat.postMessage",
  channel=slack_channel,
  text='Validated assos file ready for WP import'
)

