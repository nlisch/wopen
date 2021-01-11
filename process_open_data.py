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

def Diff(li1, li2): 
    return (list(set(li1) - set(li2))) 

url="https://opendata.paris.fr/explore/dataset/que-faire-a-paris-/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true&csv_separator=%3B"
s=requests.get(url).content
data_clean = pd.read_csv(io.StringIO(s.decode('utf-8')), error_bad_lines=False, sep=';', encoding="utf-8")

#Format correctly columns
data_clean.columns = data_clean.columns.str.replace(u"é", "e")
data_clean.columns = data_clean.columns.str.replace(' ', '_')
data_clean.columns = data_clean.columns.str.replace('è', 'e')
data_clean.columns = data_clean.columns.str.replace('à', 'a')
data_clean.columns = data_clean.columns.str.replace("'", "_")

# Get availability
data_clean = data_clean[data_clean.Occurrences.notnull()]
data_clean["Occurrences"] = data_clean.Occurrences.str.replace(";", "_")
data_clean["Occurrences"]= data_clean["Occurrences"].str.split("_") 

final = []
occurrences = data_clean["Occurrences"].tolist()

for i in range(len(occurrences)):
    print(i)
    date_to_fill = []
    for a in range(len(occurrences[i])):
        #print (test[i][a])
        date_to_process = occurrences[i][a][:10]
        date_to_process = datetime.datetime.strptime(date_to_process, '%Y-%m-%d')
        new_format = "%-d-%m-%Y"
        date_to_process = date_to_process.strftime(new_format)
        date_to_fill.append(date_to_process)
    date_to_fill = list(set(date_to_fill))
    date_to_fill = Diff(mydates, date_to_fill)
    date_to_fill = '|'.join(date_to_fill)
    #date_to_fill = 'a:2:{s:5:"dates";s:66:"' + date_to_fill + '";s:5:"price";s:0:"";}'
    final.append(date_to_fill)

#Add availability only for local file
data_clean['calendar_availability'] = final

data_clean = pd.concat([data_clean, data_clean['Coordonnees_geographiques'].str.split(',', expand=True)], axis=1)
data_clean.rename(columns = {0:'Lattitude', 1:'Longitude'}, inplace = True) 

data_clean['video_link'] = data_clean['Description'].str.extract('(https://www.youtube.com.*)\?feature=oembed')
data_clean['video_link'] = data_clean['video_link'].replace(r'embed\/', r'watch?v=', regex=True)


data_clean['Description'] = data_clean['Description'].replace(r'.div.class=.component.*</iframe></div></div>', r'', regex=True)


data_clean['Date_de_debut'] =data_clean['Date_de_debut'].str[:10]
data_clean['Date_de_fin'] =data_clean['Date_de_fin'].str[:10]


data_clean['Date_de_debut_clean'] = pd.to_datetime(data_clean['Date_de_debut'])
data_clean['Date_de_fin_clean'] = pd.to_datetime(data_clean['Date_de_fin'])

#Filter on date >= today
today = pd.Timestamp('today').normalize()
data_clean = data_clean.loc[(data_clean['Date_de_debut_clean'] >= today)]

data_clean['Duree_evenement'] = (data_clean['Date_de_fin_clean'] - data_clean['Date_de_debut_clean']).dt.days

data_clean['Date_de_debut_clean'] = data_clean['Date_de_debut_clean'].dt.strftime('%A %d %B %Y')


data_clean['arrondissement'] = data_clean['Ville']
data_clean['arrondissement'] = np.nan
data_clean.loc[data_clean['Ville'] == 'Paris', 'arrondissement'] = data_clean['Code_postal']

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
data_clean['arrondissement'] = data_clean['arrondissement'].map(dict_to_use)

data_clean['combined_address'] = data_clean['Adresse_du_lieu'].astype(str) + ', '+ data_clean['Code_postal'].astype(str) + ', '+ data_clean['Ville'].astype(str)

# Filter specific categories
categories_list = ['Concerts -> Hip-Hop',
	   'Concerts -> Rock',
       #'Animations -> Conférence / Débat',
       #'Animations -> Atelier / Cours',
       'Spectacles -> Théâtre',
       'Animations -> Stage',
       #'Animations -> Visite guidée',
       'Concerts -> Musiques du Monde',
	   'Événements -> Événement sportif',
       #'Animations -> Conférence / Débat',
       #'Animations -> Atelier / Cours',
       'Spectacles -> Théâtre',
       'Animations -> Stage',
       #'Animations -> Visite guidée',
       'Concerts -> Musiques du Monde',
       'Événements -> Événement sportif',
       'Concerts -> Chanson française',
       'Spectacles -> Jeune public',
       'Concerts -> Autre concert',
       'Expositions -> Autre expo',
       'Concerts -> Électronique',
       'Expositions -> Art Contemporain',
       'Spectacles -> Autre spectacle',
       #'Animations -> Lecture / Rencontre',
       #'Spectacles -> Projection',
       #'Événements -> Soirée / Bal',
       #'Animations -> Autre animation',
       #'Animations -> Loisirs / Jeux',
       'Concerts -> Classique',
       'Concerts -> Jazz',
       'Expositions -> Illustration / BD',
       #'Expositions -> Photographie',
       #'Événements -> Autre événement',
       #'Spectacles -> Humour',
       'Spectacles -> Danse',
       #'Événements -> Festival / Cycle',
       #'Expositions -> Histoire / Civilisations',
       #'Événements -> Salon',
       #'Spectacles -> Cirque / Art de la Rue',
       #'Animations -> Balade',
       'Expositions -> Beaux-Arts',
       'Concerts -> Soul / Funk',
       'Spectacles -> Opéra / Musical',
       #'Événements -> Fête / Parade',
       'Concerts -> Pop / Variété',
       #'Expositions -> Sciences / Techniques',
       #'Événements -> Brocante / Marché',
       #'Expositions -> Design / Mode',
       'Concerts -> Reggae',
       'Expositions -> Street-art',
       'Concerts -> Folk'
]

#Add flag for category to import
data_clean['flag_categories_to_import'] = pd.Series()
data_clean.loc[data_clean['Categorie'].isin(categories_list), 'flag_categories_to_import'] = 'yes'
data_clean.loc[~data_clean['Categorie'].isin(categories_list), 'flag_categories_to_import'] = 'no'

# Create main category column
dict_to_use_cat = {
       'Concerts -> Hip-Hop':'Musique',
       'Concerts -> Rock':'Musique',
       #'Animations -> Conférence / Débat',
       #'Animations -> Atelier / Cours',
       'Spectacles -> Théâtre':'Arts et Loisirs',
       'Animations -> Stage':'Sport, Arts et Loisirs',
       #'Animations -> Visite guidée',
       'Concerts -> Musiques du Monde':'Musique',
       'Événements -> Événement sportif':'Sport',
       'Concerts -> Chanson française':'Musique',
       'Spectacles -> Jeune public':'Eveil Enfant',
       'Concerts -> Autre concert':'Musique',
       #'Expositions -> Autre expo',
       'Concerts -> Électronique':'Musique',
       'Expositions -> Art Contemporain':'Arts et Loisirs',
       #'Spectacles -> Autre spectacle',
       #'Animations -> Lecture / Rencontre',
       #'Spectacles -> Projection',
       #'Événements -> Soirée / Bal',
       #'Animations -> Autre animation',
       #'Animations -> Loisirs / Jeux',
       'Concerts -> Classique':'Musique',
       'Concerts -> Jazz':'Musique',
       'Expositions -> Illustration / BD':'Arts et Loisirs',
       'Expositions -> Photographie':'Arts et Loisirs',
       #'Événements -> Autre événement',
       #'Spectacles -> Humour',
       'Spectacles -> Danse':'Sport',
       #'Événements -> Festival / Cycle',
       #'Expositions -> Histoire / Civilisations',
       #'Événements -> Salon',
       #'Spectacles -> Cirque / Art de la Rue',
       #'Animations -> Balade',
       'Expositions -> Beaux-Arts':'Arts et Loisirs',
       'Concerts -> Soul / Funk':'Musique',
       'Spectacles -> Opéra / Musical':'Musique',
       #'Événements -> Fête / Parade',
       'Concerts -> Pop / Variété':'Musique',
       #'Expositions -> Sciences / Techniques',
       #'Événements -> Brocante / Marché',
       #'Expositions -> Design / Mode',
       'Concerts -> Reggae':'Musique',
       'Expositions -> Street-art':'Arts et Loisirs',
       'Concerts -> Folk':'Musique'
}

data_clean['main_category'] = pd.Series()
data_clean['main_category'] = data_clean['Categorie'].map(dict_to_use_cat)

# Create main category column
dict_to_use_type_price = {
       'payant':'Payant',
       'gratuit':'Libre',
}

data_clean['Type_de_prix'] = data_clean['Type_de_prix'].map(dict_to_use_type_price)

print(data_clean.columns)

# Initialise the event table
table = BigqueryTable(
    dataset_id='crm',
    table_id='events'
)

#Write to BigQuery
table.write(
     df=data_clean
)
print ('Events table loaded in BigQuery')

#Filter dataframe on specific categories wanted
data_clean = data_clean[data_clean['Categorie'].isin(categories_list)]

# Filter Paris and good postal code
data_clean['Code_postal'] = data_clean['Code_postal'].astype(str)
data_clean = data_clean[data_clean.arrondissement.str.contains('^Paris', regex= True, na=False)]
data_clean = data_clean[data_clean.Type_de_prix.str.contains('^Libre$|^Payant$', regex= True, na=False)]
data_clean = data_clean[:0]
print(data_clean.head())
# Filter

# Create file with events to create
data_clean.to_csv('~/public/wp-content/uploads/wpallimport/files/events_paris_to_create.csv',encoding='utf-8', sep=';')



# Slack alert
slack_client = SlackClient(token=os.environ['SLACK_API_TOKEN'])
slack_message = 'Open data Paris Event updated in BigQuery & for WP Import'
slack_channel = '#script_automation'

nest_asyncio.apply()

slack_client.api_call(
  "chat.postMessage",
  channel=slack_channel,
  text=slack_message
)
print('Update sent to Slack #script_automation')

