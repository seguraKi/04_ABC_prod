# Databricks notebook source
# MAGIC %run "../../01_INIT/SBMConfig"

# COMMAND ----------

#Re-definition of the SFTP variables to be used only in Dev environment (since in Production these variables are already have the right SFTP settings
# print(rtEnvironment)
# if (rtEnvironment == "Dev"):
#   JaggaerXferSFTPuser = dbutils.secrets.get(scope = "sbm-keyvault", key = "JaggaerXferSFTPuser-prod")
#   JaggaerXferSFTPpswd = dbutils.secrets.get(scope = "sbm-keyvault", key = "JaggaerXferSFTPpswd-PROD")

# COMMAND ----------

# import datetime
# import time
# import sbm_Function
# import pandas as pd
# from datetime import datetime

try:
  now_start = datetime.now()
  title = 'Copy CSV'
  
  if rtEnvironment == 'Dev':
    sFTPPath = '/pg_demo/OutofPlatform/ABC/'
  else:
    sFTPPath = '/png_live/OutofPlatform/ABC/'
  
  sbm_Function_v2.transferFilesSFTP([], ABC_ASDWPath, sFTPPath, JaggaerXferSFTPHost, JaggaerXferSFTPuser, JaggaerXferSFTPpswd, 'get', jaggaer_rsa_key)
  
  sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start)
except Exception as ex:
  sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

try:
  now_start = datetime.now()
  title = 'Create Tables'
  
  sbm_Function_v2.saveTableDynamic(ABC_ASDWPath)
  
  sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start)
except Exception as ex:
  sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

if rtEnvironment == "Dev":
  spark.read.parquet("/mnt/sppo-refined-dev/abc_quotation_fact").createOrReplaceTempView("abc_quotation_fact")
  spark.read.parquet("/mnt/sppo-refined-dev/abc_quotation_summary_dim").createOrReplaceTempView("abc_quotation_summary_dim")
  spark.read.parquet("/mnt/sppo-refined-dev/abc_supplier_input_dim").createOrReplaceTempView("abc_supplier_input_dim")
  spark.read.parquet("/mnt/sppo-refined-dev/abc_supplier_input_inprocess_dim").createOrReplaceTempView("abc_supplier_input_inprocess_dim")
else:
  spark.read.parquet("/mnt/refined/abc_quotation_fact").createOrReplaceTempView("abc_quotation_fact")
  spark.read.parquet("/mnt/refined/abc_quotation_summary_dim").createOrReplaceTempView("abc_quotation_summary_dim")
  spark.read.parquet("/mnt/refined/abc_supplier_input_dim").createOrReplaceTempView("abc_supplier_input_dim")
  spark.read.parquet("/mnt/refined/abc_supplier_input_inprocess_dim").createOrReplaceTempView("abc_supplier_input_inprocess_dim")

# COMMAND ----------

abc_quotation_fact = """

  select 
    right(concat('0000000000', vendor_id), 10) as vendor_id
    ,right(concat('0000', plant_code), 4) as plant_code
    ,right(concat('000000000000000000', material_id), 18) as material_id
    ,abc_mat_fam_new_msm
    ,base_unit_of_measure
    ,qty_forecast_n12
    ,qty_forecast_n12_family
    ,abc_segment
    ,flag_supplier_inv_storage
    ,min_prod_qty_sc1
    ,runs_annual_sc1
    ,net_price_sc1
    ,min_prod_qty_sc2
    ,runs_annual_sc2
    ,net_price_sc2
    ,min_prod_qty_sc3
    ,runs_annual_sc3
    ,net_price_sc3
    ,min_prod_qty_sc4
    ,runs_annual_sc4
    ,net_price_sc4
    ,min_prod_qty_sc5
    ,runs_annual_sc5
    ,net_price_sc5
    ,min_prod_qty_sc6
    ,runs_annual_sc6
    ,net_price_sc6
    ,min_prod_qty_sc7
    ,runs_annual_sc7
    ,net_price_sc7
    ,min_prod_qty_sc8
    ,runs_annual_sc8
    ,net_price_sc8
    ,ticket_id
    ,material_description
    ,ticket_custom_id
    ,freight_factor
    ,price_unit_factor
    ,doc_curr_code
  from abc_quotation_fact

"""
abc_quotation_fact = spark.sql(abc_quotation_fact)
sbm_Function_v2.saveASDW(rtEnvironment, "abc_quotation_fact", abc_quotation_fact, "sbm")

# COMMAND ----------

abc_quotation_summary_dim = """

  select 
    right(concat('0000000000', vendor_id), 10) as vendor_id
    ,ticket_id
    ,modified_date
    ,ticket_owner
    ,buyer_email
    ,quotation_scenario_type
    ,ticket_custom_id
  from abc_quotation_summary_dim 

"""
abc_quotation_summary_dim = spark.sql(abc_quotation_summary_dim)
sbm_Function_v2.saveASDW(rtEnvironment, "abc_quotation_summary_dim", abc_quotation_summary_dim, "sbm")

# COMMAND ----------

abc_supplier_input_dim = """

  select 
    right(concat('0000000000', vendor_id), 10) as vendor_id
    ,right(concat('0000', plant_code), 4) as plant_code
    ,right(concat('000000000000000000', material_id), 18) as material_id
    ,material_id_phase_in
    ,date_phase_in
    ,cast(qty_forecast_n12 as decimal(10,2)) as qty_forecast_n12
    ,base_unit_of_measure
    ,cast(mm_min_lot_size as decimal(10,2)) as mm_min_lot_size
    ,cast(min_prod_qty as decimal(10,2)) as min_prod_qty
    ,abc_mat_fam_name
    ,abc_mat_fam_new
    ,abc_mat_fam_new_msm
    ,cast(restriction_min_prod_qty as decimal(10,2)) as restriction_min_prod_qty
    ,cast(restriction_max_prod_qty as decimal(10,2)) as restriction_max_prod_qty
    ,cast(material_per_pallet_qty as decimal(10,2)) as material_per_pallet_qty
    ,cast(material_shelf_life_months as decimal(10,2)) as material_shelf_life_months
    ,error_check
    ,ticket_id
    ,modified_date
    ,ticket_owner
    ,supplier_storage_flag
    ,supplier_holding_period
    ,ticket_custom_id
  from abc_supplier_input_dim

"""
abc_supplier_input_dim = spark.sql(abc_supplier_input_dim)

abc_supplier_input_dim = abc_supplier_input_dim.withColumn("restriction_min_prod_qty",F.col("restriction_min_prod_qty").cast("decimal(10,2)"))\
  .withColumn("restriction_max_prod_qty",F.col("restriction_max_prod_qty").cast("decimal(10,2)"))\
  .withColumn("material_per_pallet_qty",F.col("material_per_pallet_qty").cast("decimal(10,2)"))\
  .withColumn("material_shelf_life_months",F.col("material_shelf_life_months").cast("decimal(10,2)"))\
  .withColumn("min_prod_qty",F.col("min_prod_qty").cast("decimal(10,2)"))\
  .withColumn("qty_forecast_n12",F.col("qty_forecast_n12").cast("decimal(10,2)"))\
  .withColumn("mm_min_lot_size",F.col("mm_min_lot_size").cast("decimal(10,2)"))

(abc_supplier_input_dim.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url",url )
  .option("maxStrLength","4000")
  .option("dbtable","abc_supplier_input_dim")
  .option("restriction_min_prod_qty", "restriction_min_prod_qty decimal(10,2)")
  .option("restriction_max_prod_qty", "restriction_max_prod_qty decimal(10,2)")
  .option("material_per_pallet_qty", "material_per_pallet_qty decimal(10,2)")
  .option("material_shelf_life_months", "material_shelf_life_months decimal(10,2)")
  .option("min_prod_qty", "min_prod_qty decimal(10,2)")
  .option("qty_forecast_n12", "qty_forecast_n12 decimal(10,2)")
  .option("mm_min_lot_size", "mm_min_lot_size decimal(10,2)")
  .option("useAzureMSI", "true")
  .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
  .option("user",jdbcUsername)
  .option("password",jdbcPassword)
  .save()
)

# COMMAND ----------

abc_supplier_input_inprocess_dim = """

  select 
    right(concat('0000000000', vendor_code), 10) as vendor_id
    ,right(concat('0000', plant_code), 4) as plant_code
    ,right(concat('000000000000000000', material_id), 18) as material_id
    ,min_prod_qty
    ,abc_mat_fam_new
    ,ticket_id
    ,ticket_custom_id
    ,ticket_owner
  from abc_supplier_input_inprocess_dim 

"""
abc_supplier_input_inprocess_dim = spark.sql(abc_supplier_input_inprocess_dim)
sbm_Function_v2.saveASDW(rtEnvironment, "abc_supplier_input_inprocess_dim", abc_supplier_input_inprocess_dim, "sbm")

# COMMAND ----------

displayHTML("<span/>".join(url))

# COMMAND ----------

# DBTITLE 1,Notebook_Reg
try:
  title = 'Notebook_Reg'
  
  sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start, '1')
except Exception as ex:
  sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

dbutils.notebook.exit(notebookReturnSuccess)

# COMMAND ----------

# Mounting the table to test
spark.read.parquet("/mnt/sppo-refined-dev/abc_output").createOrReplaceTempView("abc_output")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Selecting the table to test
# MAGIC select * from abc_output
