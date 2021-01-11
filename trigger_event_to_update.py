# -*- coding: utf-8 -*-
# Update Automatic open data events
import requests

#session = requests.Session()
#session.trust_env = False
#response = session.get("TO_REPLACE_WP_ID_JOB")
#print(response)
#print ("Trigger launched")

# Update User/manuel events
session = requests.Session()
session.trust_env = False
response = session.get("TO_REPLACE_WP_ID_JOB")
print(response)
print ("Trigger launched")