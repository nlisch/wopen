    # -*- coding: utf-8 -*-

import pandas as pd
import requests
import os
import re
import io
import locale
import numpy as np
locale.setlocale(locale.LC_TIME,'fr_FR.utf8')
from slackclient import SlackClient
import nest_asyncio
import datetime

#Local function
from lib.class_bigquery import BigqueryTable
from lib.bigquery_functions import execute_sql, read_sql

from dotenv import load_dotenv
load_dotenv()
print("Packages imported")

#add dates list for availability calculation
date1 = '2019-01-01'
date2 = '2022-01-01'

mydates = pd.date_range(date1, date2)
new_format = "%-d-%m-%Y"
mydates = mydates.strftime(new_format).tolist()

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

def Diff(li1, li2): 
    return (list(set(li1) - set(li2))) 

# Initialise the event table
table = BigqueryTable(
    dataset_id='crm',
    table_id='wp_export_associations'
)

#Get products Ids of events listings
query = read_sql('python_scripts/sql/get_products_id.sql', 'event', '', '')
print('Loading data from Query below : \n\n{}'.format(query))
products = execute_sql(query, dialect='standard')
products['product_id'] = products['product_id'].astype(str)
products.columns = products.columns.str.replace(" ", "_")
products.columns = products.columns.str.lower()
products.rename(columns = {'titre':'Titre'}, inplace = True) 

#Get open date file of today
data_clean = pd.read_csv('~/public/wp-content/uploads/wpallimport/files/events_paris_to_create.csv',encoding='utf-8', sep=';', error_bad_lines=False)

# Add product ID to event and create a flag if already exists
data_clean = pd.merge(data_clean, products,
                       how='left', on=['Titre'])

data_clean['flag_already_exist'] = pd.Series()
data_clean.loc[data_clean._wp_import.str.contains('yes', regex= True, na=False), 'flag_already_exist'] = 'no'
data_clean.loc[data_clean._wp_import.str.contains('no', regex= True, na=False), 'flag_already_exist'] = 'yes'
print(data_clean[data_clean['Titre'] == 'JONATHAN BREE'])

#Update only events with no authors ( no product IDs)
data_clean_exist = data_clean.loc[data_clean['flag_already_exist'] == 'no']
data_clean_exist.to_csv('~/public/wp-content/uploads/wpallimport/files/events_paris_to_update.csv',encoding='utf-8', sep=';')

print('File update generic events updated locally')

#Filter manual events with user dates to process
update_users_events = products.copy(deep=True)
update_users_events.loc[update_users_events._wp_import.str.contains('no', regex= True, na=False), 'flag_already_exist'] = 'yes'
update_users_events = update_users_events.loc[update_users_events['flag_already_exist'] == 'yes']
#update_users_events = products[products._dates_event_user.notnull()]
update_users_events["_dates_event_user"] = update_users_events["_dates_event_user"].str.replace(" ", "")
print(update_users_events)

#Calculate availability
update_users_events["_dates_event_user"] = update_users_events["_dates_event_user"].str.strip()
update_users_events["_dates_event_user"]= update_users_events["_dates_event_user"].str.split(",") 
print(update_users_events)

final = []
final_string = []
first_date = []
occurrences = update_users_events["_dates_event_user"].tolist()
duration = []
print(occurrences)
for i in range(len(occurrences)):
    print(i)
    date_to_fill = []
    date_string = []
    dates =[]
    dates_final = []
    for a in range(len(occurrences[i])):
        #print (test[i][a])
        date_to_process = occurrences[i][a][:10]
        date_to_process = datetime.datetime.strptime(date_to_process, '%d/%m/%Y')
        new_format = "%-d-%m-%Y"
        date_processed = date_to_process.strftime(new_format)
        date_str = date_to_process.strftime("%A %d %B %Y")
        date_to_fill.append(date_processed)
        date_string.append(date_str)
        dates.append(date_to_process)
    first_date.append(min(dates))
    date_ints = set([d.toordinal() for d in dates])
    if len(date_ints) == 1:
        duration_events = str(len(date_ints)) + ' jour'
    else:  
        duration_events = str(len(date_ints)) + ' jours'
    duration.append(duration_events)
    if len(date_ints) == 1:
        dates_final.append("{}".format(min(dates).strftime("%A %d %B %Y")))
        print ("unique")
    elif max(date_ints) - min(date_ints) == len(date_ints) - 1:
        dates_final.append("Du {} au {}".format((min(dates).strftime('%A %d %B %Y')), (max(dates).strftime('%A %d %B %Y'))))
        print ("consecutive")
    else:
        dates_final.append("{}".format(', '.join(date_string)))
        print ("not consecutive")
    dates_final = ', '.join(dates_final)
    date_to_fill = list(set(date_to_fill))
    date_to_fill = Diff(mydates, date_to_fill)
    date_to_fill = '|'.join(date_to_fill)
    final.append(date_to_fill)
    final_string.append(dates_final)

print(final_string)
print(duration)

#Add availability only for local file
update_users_events['calendar_availability'] = final
update_users_events['dates_string'] = final_string
#update_users_events['dates_string'] = update_users_events['dates_string'].str.capitalize()
update_users_events['first_date'] = first_date

#Update specific fields for SEO
update_users_events['_verified'] = 'on'
update_users_events["seo_metadescription"] = update_users_events["content"]
update_users_events['_friendly_address'] = update_users_events['_address'].str.replace(' Île-de-France, France métropolitaine,', '', regex=True)
update_users_events['_friendly_address'] = update_users_events['_friendly_address'].str.replace(', France', '', regex=True)
update_users_events['_friendly_address'] = update_users_events['_friendly_address'].str.replace(', Paris', '', regex=True)
update_users_events['_friendly_address'] = update_users_events['_friendly_address'] + ', Paris'
update_users_events['_gallery_unserialized'] = update_users_events['_gallery_unserialized'].apply(clean_image)
#update_users_events['image_featured'] = update_users_events.image_url.str.split(pat = "|")
#update_users_events.loc[update_users_events.image_featured.notnull(), 'image_featured'] = update_users_events.image_featured.str[0]
update_users_events['_event_duration'] = duration

# Select column to update for manal user events
update_users_events = update_users_events[['Titre','calendar_availability','dates_string','first_date','_dates_event_user', 'seo_metadescription', '_friendly_address', 'image_featured','image_url', '_verified','_gallery_unserialized','categories','features','_event_duration']]
print(update_users_events)
update_users_events.to_csv('~/public/wp-content/uploads/wpallimport/files/events_users_to_update.csv',encoding='utf-8', sep=';')



# Slack alert
slack_client = SlackClient(token=os.environ['SLACK_API_TOKEN'])
slack_message = 'Update open data OK'
slack_channel = '#script_automation'

nest_asyncio.apply()

slack_client.api_call(
  "chat.postMessage",
  channel=slack_channel,
  text=slack_message
)
print('Update sent to Slack #script_automation')

