SELECT * 
FROM `assofinder.crm.custom_search`
WHERE date_extract = PARSE_DATE('%Y%m%d', '{0}')