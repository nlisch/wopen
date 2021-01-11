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

#Local function
from lib.class_bigquery import BigqueryTable
from lib.bigquery_functions import execute_sql, read_sql

# settings.py
from dotenv import load_dotenv
load_dotenv()
print("Packages imported")

# Slack initialisation
slack_client = SlackClient(token=os.environ['SLACK_API_TOKEN'])
slack_channel = '#script_automation'

nest_asyncio.apply()


#Number_keyword_to_update
keywords_nb = 100

# Default naming table variables
table = BigqueryTable(
    dataset_id='crm',
    table_id='custom_search'
)

#Initiate Custom search table
table.create_from_schema_partition_table(schema_path='schema/custom_search.json',
                                         partition_field='date_extract')

def diff(list1, list2):
    c = set(list1).union(set(list2))  # or c = set(list1) | set(list2)
    d = set(list1).intersection(set(list2))  # or d = set(list1) & set(list2)
    return list(c - d)

# Check if today's file has already processed
today_string = datetime.datetime.today().strftime('%Y%m%d')

#Get partitions dates for custom search table
query = read_sql('python_scripts/sql/get_partitions_custom_search.sql', '', '', '')
print('Loading data from Query below : \n\n{}'.format(query))
partitions_dates_associations_raw = execute_sql(query, dialect='legacy')
list_partitions_dates = partitions_dates_associations_raw['partition_id'].tolist()
print('Partitions Dates list {}'.format(list_partitions_dates))

if today_string in list_partitions_dates:
  print("Custom search File already processed today")

  slack_client.api_call(
  "chat.postMessage",
  channel=slack_channel,
  text='Custom search File already processed today'
)
else:
  #RNA_waldec import
  url="https://media.interieur.gouv.fr/rna/rna_waldec_20200301.zip"
  s=requests.get(url)
  mlz = zipfile.ZipFile(io.BytesIO(s.content))

  columns_data = ['id', 'id_ex', 'siret', 'rup_mi', 'gestion', 'date_creat', 'date_decla',
         'date_publi', 'date_disso', 'nature', 'groupement', 'titre',
         'titre_court', 'objet', 'objet_social1', 'objet_social2',
         'adrs_complement', 'adrs_numvoie', 'adrs_repetition', 'adrs_typevoie',
         'adrs_libvoie', 'adrs_distrib', 'adrs_codeinsee', 'adrs_codepostal',
         'adrs_libcommune', 'adrg_declarant', 'adrg_complemid',
         'adrg_complemgeo', 'adrg_libvoie', 'adrg_distrib', 'adrg_codepostal',
         'adrg_achemine', 'adrg_pays', 'dir_civilite', 'siteweb', 'publiweb',
         'observation', 'position', 'maj_time']

  li = []
  for filename in mlz.namelist():
      df = pd.read_csv(mlz.open(filename), sep=';', encoding='ISO-8859-1',  error_bad_lines=False,usecols=columns_data, low_memory=False)
      li.append(df)
  data_rna_waldec = pd.concat(li, axis=0, ignore_index=True)
  #data_rna_waldec['objet_social1'] = data_rna_waldec['objet_social1'].astype(object)
  print(data_rna_waldec.shape)

  #Mapping code
  url="https://www.data.gouv.fr/s/resources/repertoire-national-des-associations-rna/20171011-103521/Fichier_RNA_-_Nomenclature_Complete_Objet_Social_.xlsx"
  code_action = pd.read_excel(url)
  code_action['objet_social1'] = code_action['Code Objet Social']

  data_rna_waldec = pd.merge(data_rna_waldec, code_action,
                         how='left', on=['objet_social1'])
  print('RNA File processed')

  # Filter Paris
  data_rna_waldec = data_rna_waldec[data_rna_waldec.adrg_codepostal.str.contains('^75', regex= True, na=False)]
  data_rna_waldec.columns = data_rna_waldec.columns.str.replace(' ', '_')
  data_rna_waldec.columns = data_rna_waldec.columns.str.replace(':', '_')
  data_rna_waldec.columns = data_rna_waldec.columns.str.replace('-', '_')

  # Filter specific categories
  categories_list = ['culture, pratiques d\\’activités artistiques, culturelles ',
         #'promotion de l\\’art et des artistes ',
         'théâtre, marionnettes, cirque, spectacles de variété ',
         'chant choral, musique ',
         #'associations d\\’étudiants, d\\’élèves ',
         #"groupements d'entraide et de solidarité ",
         #'activités religieuses, spirituelles ou philosophiques ',
         #'représentation, promotion et défense d\\’intérêts économiques ',
         'Sports, activités de plein air ', #'interventions sociales ',
         #'santé ',
         #'éducation formation ',
         'photographie, cinéma (dont ciné-clubs) ',
         #"amicales, groupements affinitaires, groupements d'entraide (hors défense de droits fondamentaux",
         #'clubs de réflexion ',
         #'soutien et financement de partis et de campagnes électorales ',
         #'expression écrite, littérature, poésie ',
         #"amicale de personnes originaires d'un même pays (hors défense des droits des étrangers) ",
         #"échanges locaux, réseaux d\\'échanges ", 'danse ',
         #'groupement d\\’achats, groupement d\\’entreprises ',
         #'recherche médicale ',
         #'information communication ',
         #'associations caritatives, humanitaires, aide au développement, développement du bénévolat ',
         #'clubs de loisirs, relations ',
         #'associations et comités de locataires, de propriétaires, comités de logement',
         #'défense et amélioration du cadre de vie ',
         #'organisation de professions (hors caractère syndical) ',
         #'groupements professionnels',
         #'accompagnement, aide aux malades ',
         'relaxation, sophrologie',
         #'établissement de formation professionnelle, formation continue ',
         #'action socio-culturelle ',
         #'audiovisuel ',
         'arts graphiques, bande dessinée, peinture, sculpture, architecture ',
         'danse ']

  #Create label category filtered
  data_rna_waldec['filtered_cat'] = 'no'
  data_rna_waldec.loc[data_rna_waldec['Objet_Social'].isin(categories_list), 'filtered_cat'] = 'yes'

  # Initialise the custom search table
  table = BigqueryTable(
      dataset_id='crm',
      table_id='rna_waldec_filtered'
  )
  if table.exist() == False:
    #Write to BigQuery
    table.write(
       df=data_rna_waldec
    )
  
  #Filter dataframe on specific categories wanted
  data_rna_waldec.drop('filtered_cat', axis=1, inplace=True)
  data_rna_waldec = data_rna_waldec[data_rna_waldec['Objet_Social'].isin(categories_list)]

  # Get current list asso to search
  list_asso_to_search = data_rna_waldec['titre'].unique().tolist()
  list_asso_to_search = list(map(lambda x: x.lower(), list_asso_to_search))
  print('Keywords to search : {:,.0f}'.format(len(list_asso_to_search)))

  # Get list asso already searched from BigQuery
  query = read_sql('python_scripts/sql/get_custom_search_data.sql', '', '', '')
  print('Loading data from Query below : \n\n{}'.format(query))
  global_extract = execute_sql(query, dialect='standard')
  assos_searched = global_extract['searchTerms'].unique().tolist()
  assos_searched = list(map(lambda x: x.lower(), assos_searched))
  print('Keywords already searched previous days : {:,.0f}'.format(len(assos_searched)))

  remaining_assos_to_search = diff(list_asso_to_search,assos_searched)

  if remaining_assos_to_search:

    # Initialise the custom search table
    table = BigqueryTable(
        dataset_id='crm',
        table_id='custom_search'
    )
    
    print('Remaining keywords to search : {:,.0f}'.format(len(remaining_assos_to_search)))

    # Randomize keyword list
    remaining_assos_to_search = random.sample(remaining_assos_to_search, len(remaining_assos_to_search))
    # Custom search API call
    custom_search_data = adv.serp_goog(q=remaining_assos_to_search[:keywords_nb], cx=os.environ['cx'], key=os.environ['key'])
    print('Custom search API requested for : {:,.0f} queries'.format(len(remaining_assos_to_search[:keywords_nb])))

    # Save results as today's file
    custom_search_data['date_extract'] = today_string

    #Format columns correctly
    custom_search_data.columns = custom_search_data.columns.str.replace(' ', '_')
    custom_search_data.columns = custom_search_data.columns.str.replace(':', '_')
    custom_search_data.columns = custom_search_data.columns.str.replace('-', '_')
    custom_search_data['date_extract'] = pd.to_datetime(custom_search_data['date_extract'], format='%Y%m%d')
    custom_search_data['date_extract'] = custom_search_data['date_extract'].dt.date
    custom_search_data = custom_search_data[['searchTerms', 'rank', 'title', 'snippet', 'displayLink', 'link',
       'queryTime', 'totalResults', 'cacheId', 'count','date_extract']]

    # Convert column type
    custom_search_data['queryTime'] = custom_search_data['queryTime'].astype(str)
    custom_search_data['totalResults'] = custom_search_data['totalResults'].astype(int)
    custom_search_data['count'] = custom_search_data['count'].astype(int)

    print(custom_search_data.dtypes)
    print(custom_search_data)
    print(today_string)


    #Write to custom search partition table in BigQuery
    table.write_partition_table(partition_date=today_string,
                      partition_field='date_extract',
                      data=custom_search_data,
                      schema_path='schema/custom_search.json')

    # Slack alert
    slack_message = 'Custom search API script has run and updated {:,.0f} queries'.format((len(remaining_assos_to_search[:keywords_nb])))

    slack_client.api_call(
      "chat.postMessage",
      channel=slack_channel,
      text=slack_message
      )
    print('Update sent to Slack #script_automation')

  else:
    print('No Associations to search anymore')



