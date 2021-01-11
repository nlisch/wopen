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
from lib.bigquery_functions import execute_sql, read_sql

# settings.py
from dotenv import load_dotenv
load_dotenv()
print("Packages imported")


#Define functions
def clean_image(image):
    if isinstance(image, str):
        gs ="storage"
        if image.find(gs):
            #Extract first image
            image = image.split(',', 1)[0]
            #Remove extension image
            image = os.path.splitext(image)[0]
            #Remove cloud storage url
            image = image.rsplit('/', 1)[-1]
            #Remove random GS part prefix
            image = image.split('-', 1)[-1]
        else:
            image = image.split(',', 1)[0]
            image = os.path.splitext(image)[0]
        return image


# Slack initialisation
slack_client = SlackClient(token=os.environ['SLACK_API_TOKEN'])
slack_channel = '#script_automation'

nest_asyncio.apply()

# Initialise the association table
table = BigqueryTable(
    dataset_id='crm',
    table_id='wp_export_associations'
)

#Get products Ids of associations listings
query = read_sql('python_scripts/sql/get_products_id.sql', 'service', '', '')
print('Loading data from Query below : \n\n{}'.format(query))
products = execute_sql(query, dialect='standard')
products['product_id'] = products['product_id'].astype(str)
products.columns = products.columns.str.replace(" ", "_")
products.columns = products.columns.str.lower()
products.rename(columns = {'titre':'Titre'}, inplace = True) 

#Get open date file of today
data_clean = pd.read_csv('~/public/wp-content/uploads/wpallimport/files/assos_to_create.csv',encoding='utf-8', sep=';', error_bad_lines=False)
data_clean.rename(columns = {'titre':'Titre'}, inplace = True) 
print(data_clean)

# Add product ID to event and create a flag if already exists
data_clean = pd.merge(data_clean, products,
                       how='left', on=['Titre'])
print(products)
print(data_clean)

data_clean['flag_already_exist'] = pd.Series()
data_clean.loc[data_clean._wp_import.str.contains('yes', regex= True, na=False), 'flag_already_exist'] = 'no'
data_clean.loc[data_clean._wp_import.str.contains('no', regex= True, na=False), 'flag_already_exist'] = 'yes'
print(data_clean)

#Update only associations with no authors ( no product IDs), assos no updated in the interface
data_clean_no_exist = data_clean.loc[data_clean['flag_already_exist'] == 'no']
data_clean_no_exist.to_csv('~/public/wp-content/uploads/wpallimport/files/assos_to_update.csv',encoding='utf-8', sep=';')

print('updated assos file ready for WP import')

#Filter manual assos if user created it or updated it in the website interface
update_users_assos = products.copy(deep=True)
update_users_assos.loc[update_users_assos._wp_import.str.contains('no', regex= True, na=False), 'flag_already_exist'] = 'yes'
update_users_assos = update_users_assos.loc[update_users_assos['flag_already_exist'] == 'yes']

#Update specific fields for SEO
update_users_assos.loc[update_users_assos['author_id'].isin([0]) == False, '_verified'] = 'on'
update_users_assos["seo_metadescription"] = update_users_assos["content"]
print(update_users_assos['_friendly_address'])
update_users_assos['street'] = update_users_assos['_address'].str.extract(r'^(.+?,.+?),', expand=True)
update_users_assos['new_address'] = update_users_assos['_address'].str.replace(' Île-de-France, France métropolitaine,', '', regex=True)
update_users_assos['new_address'] = update_users_assos['new_address'].str.replace(', France', '', regex=True)
update_users_assos['new_address'] = update_users_assos['new_address'].str.replace(', Paris', '', regex=True)
update_users_assos['postcode'] = update_users_assos['new_address'].str.extract(r'.*,(.*)$', expand=True)
update_users_assos['new_address'] = update_users_assos['street'] + ', ' + update_users_assos['postcode'] + ', Paris'
update_users_assos.loc[update_users_assos['_friendly_address'].isnull(), '_friendly_address'] = update_users_assos['new_address']
print(update_users_assos['_friendly_address'])
update_users_assos['_gallery_unserialized'] = update_users_assos['_gallery_unserialized'].apply(clean_image)
#update_users_assos['image_featured'] = update_users_assos.image_url.str.split(pat = "|")
#update_users_assos.loc[update_users_assos.image_featured.notnull(), 'image_featured'] = update_users_assos.image_featured.str[0]


# Select column to update for manual user assos
update_users_assos = update_users_assos[['Titre','seo_metadescription', '_friendly_address', 'image_featured','image_url', '_verified','_gallery_unserialized','categories','features']]
print(update_users_assos)
update_users_assos.to_csv('~/public/wp-content/uploads/wpallimport/files/assos_users_to_update.csv',encoding='utf-8', sep=';')

slack_client.api_call(
  "chat.postMessage",
  channel=slack_channel,
  text='Update assos OK'
)

