# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook info:
# MAGIC
# MAGIC -  **Business context explanation:**  A table is generated where the information of the price scales is saved to be used in other analyzes to avoid rework for the user.
# MAGIC - **Base tables**: 
# MAGIC      - abc_scale_fact
# MAGIC      - abc_scale_summary_dim
# MAGIC      - geo_dim
# MAGIC      - plant_dim
# MAGIC      - active_agreement_fact
# MAGIC - **Developers involved:** Kenneth Aguilar
# MAGIC - **Frequency of running:**
# MAGIC   1. Daily

# COMMAND ----------

# MAGIC %run "../../01_INIT/SBMConfig"

# COMMAND ----------

# MAGIC %run "../../01_INIT/SBM_Logger"

# COMMAND ----------

set_log_identifiers(solution_name="ABC", pipeline_id=5, process_id=5)
log_process_start()

# COMMAND ----------

from connectors import JaggaerConnector
from pathlib import Path
jaggaer_connector = JaggaerConnector("prod")

# COMMAND ----------

# if rtEnvironment == 'Dev':
# spark.read.parquet("/mnt/sppo-refined-dev/abc_scale_fact").createOrReplaceTempView("abc_scale_fact")
# spark.read.parquet("/mnt/sppo-refined-dev/abc_scale_summary_dim").createOrReplaceTempView("abc_scale_summary_dim")
# else:
spark.read.parquet("/mnt/sbm_refined/abc_scale_fact/").createOrReplaceTempView("abc_scale_fact")
spark.read.parquet("/mnt/sbm_refined/abc_scale_summary_dim/").createOrReplaceTempView("abc_scale_summary_dim")
spark.read.parquet("/mnt/mda-prod/site_attr_dim").createOrReplaceGlobalTempView("site_attr_dim")
spark.read.parquet("/mnt/mda-prod/site_attr_dim").createOrReplaceGlobalTempView("site_attr_dim")
spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y'").createOrReplaceGlobalTempView("active_agreement_fact")

#____________________________________________________________________

# if rtEnvironment == 'Dev':
#   spark.read.parquet("/mnt/sppo-refined-dev/abc_scale_fact").createOrReplaceTempView("abc_scale_fact")
#   spark.read.parquet("/mnt/sppo-refined-dev/abc_scale_summary_dim").createOrReplaceTempView("abc_scale_summary_dim")
# else:
#   spark.read.parquet("/mnt/refined/abc_scale_fact/").createOrReplaceTempView("abc_scale_fact")
#   spark.read.parquet("/mnt/refined/abc_scale_summary_dim/").createOrReplaceTempView("abc_scale_summary_dim")
#   spark.read.parquet("/mnt/refined/geo_dim").createOrReplaceGlobalTempView("geo_dim")
#   spark.read.parquet("/mnt/refined/plant_dim").createOrReplaceGlobalTempView("plant_dim")
#   spark.read.parquet("/mnt/refined/active_agreement_fact").where("source_list_flag = 'Y'").createOrReplaceGlobalTempView("active_agreement_fact")

# if rtEnvironment == 'Dev':
#   spark.read.parquet("/mnt/mda-prod/abc_scale_fact").createOrReplaceTempView("abc_scale_fact")
#   spark.read.parquet("/mnt/mda-prod/abc_scale_summary_dim").createOrReplaceTempView("abc_scale_summary_dim")
# else:
#   spark.read.parquet("/mnt/mda-prod/abc_scale_fact/").createOrReplaceTempView("abc_scale_fact")
#   spark.read.parquet("/mnt/mda-prod/abc_scale_summary_dim/").createOrReplaceTempView("abc_scale_summary_dim")
#   spark.read.parquet("/mnt/mda-prod/geo_dim").createOrReplaceGlobalTempView("geo_dim")
#   spark.read.parquet("/mnt/mda-prod/plant_dim").createOrReplaceGlobalTempView("plant_dim")
#   spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y'").createOrReplaceGlobalTempView("active_agreement_fact")

############# jaggaer_view ##############################
view_type = 'jaggaer_view'

#SBM_Tables_v2.createTable('vw_sbm_plant_dim', view_type)

# COMMAND ----------

# GCAS Level
sql = """

select 
  CAST(asf.vendor_id as INT) as vendor_id
  ,concat(pd.plant_code, ': ', pd.plant_name) as plant_code
  ,cast(asf.material_id as int) as material_id
  ,asf.scale_uom
  ,asf.mpq_scale_qty
  ,asf.doc_curr_code
  ,asf.scale_price
  ,asf.price_unit_factor
  ,ifnull(aaf.business_unit, '') as business_unit
  ,pd.plant_code as only_plant_code
  ,pd.prev_region_name as region
  ,ssd.scale_scenario_type
from abc_scale_fact asf
inner join abc_scale_summary_dim ssd
  on ssd.ticket_id = asf.ticket_id
left join global_temp.vw_sbm_plant_dim pd
  on pd.plant_code = asf.plant_code
left join global_temp.active_agreement_fact aaf
  on aaf.plant_code = asf.plant_code
  and cast(aaf.material_id as int) = cast(asf.material_id as int)
  and cast(aaf.purchase_vendor_id as int) = cast(asf.vendor_id as int)
where ssd.scale_scenario_type like '1'
"""
abc_scale_fact_template = spark.sql(sql)
# abc_scale_fact_template.count()
# display(abc_scale_fact_template)

# COMMAND ----------

# Family Level
sql = """

select 
  CAST(asf.vendor_id AS INT) as vendor_id
  ,concat(pd.plant_code, ': ', pd.plant_name) as plant_code
  ,asf.scale_fam
  ,asf.scale_uom
  ,asf.mpq_scale_qty
  ,asf.doc_curr_code
  ,asf.scale_price
  ,asf.price_unit_factor
  ,ifnull(aaf.business_unit, '') as business_unit
  ,pd.plant_code as only_plant_code
  ,pd.prev_region_name as region
  ,ssd.scale_scenario_type
from abc_scale_fact asf
inner join abc_scale_summary_dim ssd
  on ssd.ticket_id = asf.ticket_id
left join global_temp.vw_sbm_plant_dim pd
  on pd.plant_code = asf.plant_code
left join global_temp.active_agreement_fact aaf
  on aaf.plant_code = asf.plant_code
  and cast(aaf.material_id as int) = cast(asf.material_id as int)
  and cast(aaf.purchase_vendor_id as int) = cast(asf.vendor_id as int)
where ssd.scale_scenario_type like '2'
"""
abc_scale_fact_template_fam = spark.sql(sql)
# display(abc_scale_fact_template_fam)

# COMMAND ----------

# try:
#   now_start = datetime.now()
#   title = 'export_jaggaer_view_GenerateCSV'
  
#   #Below code splits the files
#   files = sbm_Function_v2.splitDFtoCSV(abc_scale_fact_template, "/mnt/temp/sbm/", "abc_scale_fact_template", 100000)
#   print(files)

#   # #Below code SFTP transfers the split files list from above
#   # transfered_files = sbm_Function_v2.transferFiles(files, 'IntoPlatform/abc_scale_fact_template', JaggaerXferSFTPHost, JaggaerXferSFTPuser, JaggaerXferSFTPpswd, 'put', jaggaer_rsa_key)
#   # print(transfered_files)

#   sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start)
# except Exception as ex:
#   sbm_Function_v2.sendMessageWithDF("", "Error found", 'Error found: </br></br> Error generating and uploading the abc_scale_fact_template csv file...</br>'+title+ '</br>'+NotebookName+'. </br></br>'+str(ex), title )
#   pythonCustomExceptionHandler(ex, "Error generating and uploading the abc_scale_fact_template csv file...")  
#   sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)


# try:
#   now_start = datetime.now()
#   title = 'export_jaggaer_view_GenerateCSV'
  
#   #Below code splits the files
#   files = sbm_Function_v2.splitDFtoCSV(abc_scale_fact_template_fam, "/mnt/temp/sbm/", "abc_scale_fact_template_fam", 100000)
#   print(files)

#   # #Below code SFTP transfers the split files list from above
#   # transfered_files = sbm_Function_v2.transferFiles(files, 'IntoPlatform/abc_scale_fact_template_fam', JaggaerXferSFTPHost, JaggaerXferSFTPuser, JaggaerXferSFTPpswd, 'put', jaggaer_rsa_key)
#   # print(transfered_files)

#   sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start)
# except Exception as ex:
#   sbm_Function_v2.sendMessageWithDF("", "Error found", 'Error found: </br></br> Error generating and uploading the abc_scale_fact_template_fam csv file...</br>'+title+ '</br>'+NotebookName+'. </br></br>'+str(ex), title )
#   pythonCustomExceptionHandler(ex, "Error generating and uploading the abc_scale_fact_template_fam csv file...")  
#   sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

from datetime import datetime
files = sbm_Function_v2.splitDFtoCSV(abc_scale_fact_template, "/mnt/sbm_stage/sbm/", "abc_scale_fact_template", 100000)
remotepath_parent = 'IntoPlatform/abc_scale_fact_template'
list_of_localpaths = files
for f in list_of_localpaths:
  path_to_file = Path(f)
  file_name = path_to_file.name
  jaggaer_connector.put_file(str(path_to_file), remotepath_parent+"/"+file_name)
  os.rename(str(f), str(f)[:-4] + datetime.today().strftime('_%Y_%m_%d_%H_%M'))

# COMMAND ----------

files = sbm_Function_v2.splitDFtoCSV(abc_scale_fact_template_fam, "/mnt/sbm_stage/sbm/", "abc_scale_fact_template_fam", 100000)
remotepath_parent = 'IntoPlatform/abc_scale_fact_template_fam'
list_of_localpaths = files
for f in list_of_localpaths:
  path_to_file = Path(f)
  file_name = path_to_file.name
  jaggaer_connector.put_file(str(path_to_file), remotepath_parent+"/"+file_name)
  os.rename(str(f), str(f)[:-4] + datetime.today().strftime('_%Y_%m_%d_%H_%M'))


# COMMAND ----------

log_process_complete(notebookReturnSuccess)

# COMMAND ----------

dbutils.notebook.exit(notebookReturnSuccess)
