# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

import pyspark.sql.functions as F
import requests, json


# COMMAND ----------

#Widget for selecting region
dbutils.widgets.dropdown("environment","PROD",["QA","PROD"],'select environment')

# COMMAND ----------

#Get selected environment from widget
selected_environment = dbutils.widgets.get("environment")

# COMMAND ----------

#Define regions based on environment
rr_region = {
  "QA" : ['AS','L6','F6','N6'],
  "PROD" : ['AS','L6','F6','N6']
}

# COMMAND ----------

#Remove existing widget if it exists

try:
  dbutils.widgets.remove("rr_region")
except:
  pass

# COMMAND ----------

#create a widget for region selection based on the selected environment

dbutils.widgets.dropdown("rr_region", rr_region[selected_environment][3],rr_region[selected_environment],"select region")

# COMMAND ----------

#Widget for selecting region
#dbutils.widgets.dropdown("rr_region",['N6','AS','L6','F6'],'select_region')

# COMMAND ----------

#read region input
rr_region = dbutils.widgets.get('rr_region')

# COMMAND ----------

F6P_parquet = spark.read.parquet("/mnt/sbm_refined/abc_kinaxis_input_fact/source_system_code=F6PS4H/")
L6P_parquet = spark.read.parquet("/mnt/sbm_refined/abc_kinaxis_input_fact/source_system_code=L6P430/")
N6P_parquet = spark.read.parquet("/mnt/sbm_refined/abc_kinaxis_input_fact/source_system_code=N6P420/")

anp = spark.read.parquet("/mnt/sbm_refined/abc_kinaxis_input_fact/source_system_code=A6P430/")
a6p = spark.read.parquet("/mnt/sbm_refined/abc_kinaxis_input_fact/source_system_code=ANP430/")
AC_parquet = anp.union(a6p)

# COMMAND ----------

# Adding it here for future reference, not used yet
client_filter = ['410', '420', '430', 'S4H']
box_filter = ['F7P410', 'F6P430', 'F5P420']
material_vendor_filter = ['', ' ', '--']

client_filter_sql = ', '.join("'" + c + "'" for c in client_filter)
box_filter_sql = ', '.join("'" + b + "'" for b in box_filter)
material_vendor_filter_sql = ', '.join("'" + m + "'" for m in material_vendor_filter)

# COMMAND ----------

credentials = {

  "PROD" : {
    "AS" : {
      "client_id" : "2b305b9c25f46769fee779915cb98f80",
      "client_secret" : "241cab39d834c3449b56f063a099c65f54312639041f413f635e2f70ed06d2ae",
      "rr_compId" : "PNGP08_PRD3",
      "rr_import" : "/integration/v1/data/workbook/import",
      "rr_region" : "AS",
      "rr_scenario" : "Data Persistence",
      "rr_script" : "/integration/v1/script/Public/PG%20Material%20ABC%20Tool%20Import",
      "rr_token" : "/oauth2/token",
      "rr_url" : "https://na2.kinaxis.net/",
      "path" : AC_parquet
  },
    "L6" : {
      "client_id" : "2b305b9c25f46769fee779915cb98f80",
      "client_secret" : "241cab39d834c3449b56f063a099c65f54312639041f413f635e2f70ed06d2ae",
      "rr_compId" : "PNGP08_PRD3",
      "rr_import" : "/integration/v1/data/workbook/import",
      "rr_region" : "L6",
      "rr_scenario" : "Data Persistence",
      "rr_script" : "/integration/v1/script/Public/PG%20Material%20ABC%20Tool%20Import",
      "rr_token" : "/oauth2/token",
      "rr_url" : "https://na2.kinaxis.net/",
      "path" : L6P_parquet
  },
    "F6" : {
      "client_id" : "ae2cbe2634a485756269b550f70d110f",
      "client_secret" : "4a552941498b08d1184d0690b671c4fee90dfccd4d5a4e196452cce051876db2",
      "rr_compId" : "PNGP05_PRD2",
      "rr_import" : "/integration/v1/data/workbook/import",
      "rr_region" : "F6",
      "rr_scenario" : "Data Persistence",
      "rr_script" : "/integration/v1/script/Public/PG%20Material%20ABC%20Tool%20Import",
      "rr_token" : "/oauth2/token",
      "rr_url" : "https://na2.kinaxis.net/",
      "path" : F6P_parquet
  },
    "N6" : {
      "client_id" : "ce2f0478220fc7503c1952a4bde8bac9",
      "client_secret" : "2a0105dba2eea6d18d3be1e0dc76f4273142e42908e6c79bff6c76a27ff7179f",
      "rr_compId" : "PNGP02_PRD",
      "rr_import" : "/integration/v1/data/workbook/import",
      "rr_region" : "N6",
      "rr_scenario" : "Data Persistence",
      "rr_script" : "/integration/v1/script/Public/PG%20Material%20ABC%20Tool%20Import",
      "rr_token" : "/oauth2/token",
      "rr_url" : "https://na2.kinaxis.net/",
      "path" : N6P_parquet
  } },
    "QA" : {
    "AS" : {
      "client_id" : "84de914105ba81b7ae127a6e7095a281",
      "client_secret" : "ac5fd1db1f4f80150a08ee69d17905c54072b8daf8fb0aff7a0fd5e6decb8102",
      "rr_compId" : "PNGT11_QA3",
      "rr_import" : "/integration/v1/data/workbook/import",
      "rr_region" : "AS",
      "rr_scenario" : "Data Persistence",
      "rr_script" : "/integration/v1/script/Public/PG%20Material%20ABC%20Tool%20Import",
      "rr_token" : "/oauth2/token",
      "rr_url" : "https://na1.kinaxis.net/",
      "path" : AC_parquet
  },
    "L6" : {
      "client_id" : "84de914105ba81b7ae127a6e7095a281",
      "client_secret" : "ac5fd1db1f4f80150a08ee69d17905c54072b8daf8fb0aff7a0fd5e6decb8102",
      "rr_compId" : "PNGT11_QA3",
      "rr_import" : "/integration/v1/data/workbook/import",
      "rr_region" : "L6",
      "rr_scenario" : "Data Persistence",
      "rr_script" : "/integration/v1/script/Public/PG%20Material%20ABC%20Tool%20Import",
      "rr_token" : "/oauth2/token",
      "rr_url" : "https://na1.kinaxis.net/",
      "path" : L6P_parquet
  },
    "F6" : {
      "client_id" : "96639b61c944039dae7210d9e4926912",
      "client_secret" : "3f848b90e300ab96675a0def6667dd9a74c297a11c9aa6e3648732774e89e241",
      "rr_compId" : "PNGT08_QA2",
      "rr_import" : "/integration/v1/data/workbook/import",
      "rr_region" : "F6",
      "rr_scenario" : "Data Persistence",
      "rr_script" : "/integration/v1/script/Public/PG%20Material%20ABC%20Tool%20Import",
      "rr_token" : "/oauth2/token",
      "rr_url" : "https://na1.kinaxis.net/",
      "path" : F6P_parquet
  },
    "N6" : {
      "client_id" : "96639b61c944039dae7210d9e4926912",
      "client_secret" : "3f848b90e300ab96675a0def6667dd9a74c297a11c9aa6e3648732774e89e241",
      "rr_compId" : "PNGT08_QA2",
      "rr_import" : "/integration/v1/data/workbook/import",
      "rr_region" : "N6",
      "rr_scenario" : "Data Persistence",
      "rr_script" : "/integration/v1/script/Public/PG%20Material%20ABC%20Tool%20Import",
      "rr_token" : "/oauth2/token",
      "rr_url" : "https://na1.kinaxis.net/",
      "path" : N6P_parquet
  } }

}




# COMMAND ----------

#credentials[selected_environment][rr_region]

# COMMAND ----------

print('client_id: ' + credentials[selected_environment][rr_region]['client_id'])
print('client_secret: ' + credentials[selected_environment][rr_region]['client_secret'])
print('rr_scenario: ' + credentials[selected_environment][rr_region]['rr_scenario'])
print('rr_url: ' + credentials[selected_environment][rr_region]['rr_url'])
print('rr_compId: ' + credentials[selected_environment][rr_region]['rr_compId'])
print('rr_script: ' + credentials[selected_environment][rr_region]['rr_script'])
print('rr_import: ' + credentials[selected_environment][rr_region]['rr_import'])
print('rr_token: ' + credentials[selected_environment][rr_region]['rr_token'])
print(f'path: ' + str(credentials[selected_environment][rr_region]['path']))





# COMMAND ----------

client_id = credentials[selected_environment][rr_region]['client_id']
client_secret = credentials[selected_environment][rr_region]['client_secret']
rr_scenario = credentials[selected_environment][rr_region]['rr_scenario']
rr_url = credentials[selected_environment][rr_region]['rr_url']
rr_compId = credentials[selected_environment][rr_region]['rr_compId']
rr_script = credentials[selected_environment][rr_region]['rr_script']
rr_import = credentials[selected_environment][rr_region]['rr_import']
rr_region = credentials[selected_environment][rr_region]['rr_region']
rr_token = credentials[selected_environment][rr_region]['rr_token']
path = credentials[selected_environment][rr_region]['path']


# COMMAND ----------

importURL = rr_url + rr_compId + rr_import
scriptURL = rr_url + rr_compId + rr_script
tokenURL = rr_url + rr_compId + rr_token

def getToken(url, clientId, clientSecret):
  headers = { "Content-Type": "application/x-www-form-urlencoded" }
  body = { "grant_type":"client_credentials" }
  auth = requests.auth.HTTPBasicAuth(clientId, clientSecret)
  response = requests.post(url=url, data=body, auth=auth, headers=headers)
  if(response.status_code == 200):
    data = response.json()
    token = data['access_token']
    return token
  else:
    print("Failed to generate bearer token.")


def modifyData(url, token):
  headers = { 
    "Content-type": "application/json",
    "Authorization": "Bearer " + token
  }
  body = f'{{ "Scenario": "{rr_scenario} {rr_region}"}}'
  response = requests.post(url=url, data=body, headers=headers)
  data = response.json()
#   print(f'== {action} Data ==')
  print(data['Console'])


def getData():
#   refresh = spark.sql("refresh table abc_rapidresponse.abc_kinaxis_input_fact;") # Replace the database and table

#   sql_script = """select * from """+rr_region+"""kinaxis_input"""
#   refresh = spark.sql("refresh table abc_rapidresponse.abc_kinaxis_input_fact;") # Replace the database and table
#   data = spark.sql("select * from abc_rapidresponse.abc_kinaxis_input_fact limit 10000").agg(F.concat_ws(",",F.collect_list("jsonObject"))) # Replace the database and table
  
  #data = path.agg(F.concat_ws(",",F.collect_list("jsonObject"))) # Replace the database and table
  data = path.agg(F.concat_ws(",",F.collect_list(
    F.when(
      F.col("jsonObject").isNotNull(),
      F.regexp_replace(F.col("jsonObject"), r'[^\x00-\x7F]','')
    ).otherwise(F.col("jsonObject"))
  )).alias("cleanedJsonObjects")
  )
#   dataTest = spark.sql(sql_script)

  print("- Table has been refresh -\n")
  return data


def uploadData(url, token, testData):
  headers = { 
    "Content-type": "application/json",
    "Authorization": "Bearer " + token
  }
  body = f'''
  {{
    "Scenario": {{
        "Name": "{rr_scenario} {rr_region}",
        "Scope": "Public"
    }},
    "WorkbookParameters": {{
        "Workbook": {{
            "Name": "PG Material ABC Tool Import",
            "Scope": "Public"
        }},
        "SiteGroup": "All Sites",
        "Filter": {{
            "Name": "All Parts",
            "Scope": "Public"
        }},
        "WorksheetNames": ["Raw Data"],
        "VariableValues": {{
          "ShowHistoricalData": false
        }}
    }},
    "Rows": [
      {testData.collect()[0][0]}
    ]
  }}
  '''
#   body += testData.collect()[0][0]
#   body += ']}'
  response = requests.post(url=url, data=body, headers=headers)
  if(response.status_code == 200):
    data = response.json()
    details = data['Worksheets'][0]
    print("== Upload Data ==")
    print("Worksheet: " + details['WorksheetName'])
    print("Imported: " + str(details['ImportedRowCount']))
    print("Inserted: " + str(details['InsertedRowCount']))
    print("Modified: " + str(details['ModifiedRowCount']))
    print("Error: " + str(details['ErrorRowCount']) + "\n")
  else:
#     print("Failed to upload data.")
      print(response.text)
token = getToken(tokenURL, client_id, client_secret)
testData = getData()
uploadData(importURL, token, testData)
modifyData(scriptURL, token)




# COMMAND ----------

# have a notebooksuccess or notebookfail similar to other SBM framework, to assess if this notebook failed or succeeded.
# this success & fail will be based on HTTP RESPONSE against the HTTP POST action.

# additional check if we can use the email framework to generate an email at end of execution of this notebook. ALTERNATIVELY, you trigger something in ADF to send email if the notebook can show failure there.
