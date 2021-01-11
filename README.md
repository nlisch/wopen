# wopen

## ETL

Associations:

1. ETL Associations open data from gouv.data
2. Check if a Facebook account exists for each daily ( Google Search API )
3. Process levenshtein similarity score to isolate active facebook accounts
4. Extract to gsheets associations list and calculate additionnal data such lattitute and longitude coordinate based on adress.
5. Upload to wordpress WP Import and create listing page automatically

Events:

1. ETL Associations open data from Paris open data
2. Upload to wordpress WP Import and create listing page automatically


## How to run scripts ?

Manually, otherwise, script could be trigerred through cron JOBs such these examples below:
```
30 6 * * * /usr/bin/python3 /www/assofinder_397/python_scripts/process_open_data.py >/dev/null 2>&1
35 6 * * * /usr/bin/python3 /www/assofinder_397/python_scripts/trigger.py >/dev/null 2>&1
```

## How to monitor execution ?

Slack alert are sent each time the job execue properly.
Staging dataset are created in BigQuery for each execution and could be troubleshoot creating datastudio monitoring dashboard for example.



## Python Version

3.7

## Environment Variables Required 

To execute the scripts you need to add `.env` file to the root of the project with these variables (values are examples):

A google Cloud platform project needs to be set up first.

```
## BigQuery
BIGQUERY_PROJECT=XXXX
BIGQUERY_ACCESS_TOKEN_PATH=XXXX.json

## Slack
SLACK_API_TOKEN = XXXX

#GSHEET
cx = XXXX
key = XXXX
GOOGLE_APPLICATION_CREDENTIALS=XXXX.json
GOOGLE_BUCKET=XXXX

#PYTHON
PYTHONPATH=XXXX
```