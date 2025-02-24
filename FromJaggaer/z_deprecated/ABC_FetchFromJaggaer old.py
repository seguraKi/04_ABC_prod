# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook info:
# MAGIC
# MAGIC -  **Business context explanation:**  The Notebook takes the tables from Jaggaer and inserts them into the ASDW
# MAGIC - **Base tables**: 
# MAGIC     - abc_quotation_summary_dim
# MAGIC     - abc_supplier_input_dim
# MAGIC     - abc_supplier_input_inprocess_dim
# MAGIC     - abc_quotation_fam_fact
# MAGIC     - abc_trigger_summary_dim
# MAGIC     - abc_fcst_override_fact
# MAGIC     - abc_scale_fact
# MAGIC     - abc_scale_summary_dim
# MAGIC - **Developers involved:** Kenneth Aguilar
# MAGIC - **Frequency of running:**
# MAGIC   1. Every 6 hours
# MAGIC
# MAGIC From sFTP -> Storage Account, save it to /mnt/raw/*
# MAGIC Same process applies
# MAGIC Every output of every DF needs to be saved in /mnt/refined/*
# MAGIC

# COMMAND ----------

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
  spark.read.parquet("/mnt/sppo-refined-dev/abc_quotation_fam_fact").createOrReplaceTempView("abc_quotation_fam_fact")
  spark.read.parquet("/mnt/sppo-refined-dev/abc_trigger_summary_dim").createOrReplaceTempView("abc_trigger_summary_dim")
  spark.read.parquet("/mnt/sppo-refined-dev/abc_fcst_override_fact").createOrReplaceTempView("abc_fcst_override_fact")
  spark.read.parquet("/mnt/sppo-refined-dev/abc_scale_fact").createOrReplaceTempView("abc_scale_fact")
  spark.read.parquet("/mnt/sppo-refined-dev/abc_scale_summary_dim").createOrReplaceTempView("abc_scale_summary_dim")
  
  
else:
  spark.read.parquet("/mnt/refined/abc_quotation_fact").createOrReplaceTempView("abc_quotation_fact")
  spark.read.parquet("/mnt/refined/abc_quotation_summary_dim").createOrReplaceTempView("abc_quotation_summary_dim")
  spark.read.parquet("/mnt/refined/abc_supplier_input_dim").createOrReplaceTempView("abc_supplier_input_dim")
  spark.read.parquet("/mnt/refined/abc_supplier_input_inprocess_dim").createOrReplaceTempView("abc_supplier_input_inprocess_dim")
  spark.read.parquet("/mnt/refined/abc_quotation_fam_fact").createOrReplaceTempView("abc_quotation_fam_fact")  
  spark.read.parquet("/mnt/refined/abc_trigger_summary_dim").createOrReplaceTempView("abc_trigger_summary_dim")
  spark.read.parquet("/mnt/refined/abc_fcst_override_fact").createOrReplaceTempView("abc_fcst_override_fact")
  spark.read.parquet("/mnt/refined/abc_scale_fact").createOrReplaceTempView("abc_scale_fact")
  spark.read.parquet("/mnt/refined/abc_scale_summary_dim").createOrReplaceTempView("abc_scale_summary_dim")

# COMMAND ----------

abc_scale_summary_dim = """

  select 
    right(concat('0000000000', vendor_id), 10) as vendor_id
    ,ticket_id
    ,ticket_custom_id
    ,supplier_input_ticket_id
    ,modified_date
    ,ticket_owner
    ,scale_scenario_type
    ,supplier_deadline
    ,ticket_status
    ,buyer_email
    ,supplier_email
    ,flag_supplier_inv_storage
    ,ticket_title
  from abc_scale_summary_dim
"""
abc_scale_summary_dim = spark.sql(abc_scale_summary_dim)
(abc_scale_summary_dim.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url",url )
  .option("maxStrLength","4000")
  .option("dbtable","abc_scale_summary_dim")
  .option("useAzureMSI", "true")
  .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
  .option("user",jdbcUsername)
  .option("password",jdbcPassword)
  .save()
)

# COMMAND ----------

abc_scale_fact = """

  select distinct 
    right(concat('0000000000', sf.vendor_id), 10) as vendor_id
    ,right(concat('0000', sf.plant_code), 4) as plant_code
    ,right(concat('000000000000000000', sf.material_id), 18) as material_id
    ,sf.scale_fam
    ,sf.scale_uom
    ,cast(sf.mpq_scale_qty as decimal(12,2)) mpq_scale_qty
    ,sf.doc_curr_code
    ,cast(sf.scale_price as decimal(12,2)) scale_price
    ,cast(sf.price_unit_factor as decimal(12,2)) price_unit_factor
    ,sf.valid_start_date
    ,sf.ticket_id
  from abc_scale_fact sf
  inner join abc_scale_summary_dim ssd
    on ssd.ticket_id = sf.ticket_id
  where ssd.scale_scenario_type like '1' and (sf.material_id not like '' or sf.material_id not like ' ')

  Union all 

  select distinct 
    right(concat('0000000000', sf.vendor_id), 10) as vendor_id
    ,right(concat('0000', sf.plant_code), 4) as plant_code
    ,right(concat('000000000000000000', sf.material_id), 18) as material_id
    ,sf.scale_fam
    ,sf.scale_uom
    ,cast(sf.mpq_scale_qty as decimal(12,2)) mpq_scale_qty
    ,sf.doc_curr_code
    ,cast(sf.scale_price as decimal(12,2)) scale_price
    ,cast(sf.price_unit_factor as decimal(12,2)) price_unit_factor
    ,sf.valid_start_date
    ,sf.ticket_id
  from abc_scale_fact sf
  inner join abc_scale_summary_dim ssd
    on ssd.ticket_id = sf.ticket_id
  where ssd.scale_scenario_type like '2' and (sf.scale_fam not like '' or sf.scale_fam not like ' ')
"""
abc_scale_fact = spark.sql(abc_scale_fact)
(abc_scale_fact.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url",url )
  .option("maxStrLength","4000")
  .option("dbtable","abc_scale_fact")
  .option("mpq_scale_qty","mpq_scale_qty decimal(12,2)") 
  .option("scale_price","scale_price decimal(12,2)") 
  .option("price_unit_factor","price_unit_factor decimal(12,2)") 
  .option("useAzureMSI", "true")
  .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
  .option("user",jdbcUsername)
  .option("password",jdbcPassword)
  .save()
)

# COMMAND ----------

abc_fcst_override_fact = """

  select
    right(concat('0000000000', vendor_id), 10) as vendor_id
    ,right(concat('0000', plant_code), 4) as plant_code
    ,right(concat('000000000000000000', material_id), 18) as material_id
    ,base_unit_of_measure
    ,cast(qty_forecast_n1y as decimal(12,2)) as qty_forecast_n1y
    ,cast(qty_forecast_override as decimal(12,2)) as qty_forecast_override
    ,ticket_id
  from abc_fcst_override_fact
"""
abc_fcst_override_fact = spark.sql(abc_fcst_override_fact)
(abc_fcst_override_fact.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url",url )
  .option("maxStrLength","4000")
  .option("dbtable","abc_fcst_override_fact")
  .option("qty_forecast_n1y","qty_forecast_n1y decimal(12,2)") 
  .option("qty_forecast_override","qty_forecast_override decimal(12,2)") 
  .option("useAzureMSI", "true")
  .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
  .option("user",jdbcUsername)
  .option("password",jdbcPassword)
  .save()
)

# COMMAND ----------

abc_trigger_summary_dim = """

  select 
    right(concat('0000000000', vendor_id), 10) as vendor_id
    ,ticket_id
    ,modified_date
    ,ticket_owner
    ,supplier_storage_flag
    ,cast(supplier_holding_period as int) as supplier_holding_period
    ,commercial_transparency_flag
    ,production_families_flag
    ,forecast_override_flag
    ,ticket_custom_id
    ,ticket_title
    ,ticket_status
    ,CAST(pipo_horizon_days AS INT) AS pipo_horizon_days
    ,CAST(restriction_max_mpq_dfc AS INT) AS restriction_max_mpq_dfc
    ,CAST(current_warehouse_flag AS VARCHAR(3)) AS current_warehouse_flag
    ,CAST(pallet_qty AS BIGINT) AS pallet_qty
    ,CAST(warehouse_per_pallet_amt_usd AS DECIMAL(18,3)) as warehouse_per_pallet_amt_usd
    ,CAST(transportation_per_pallet_amt_usd AS DECIMAL(18,3)) as transportation_per_pallet_amt_usd
    ,CAST (external_warehouse_flag AS VARCHAR(3)) AS external_warehouse_flag 
    ,CAST(current_pallet_qty AS DECIMAL(18,3)) as current_pallet_qty
    ,CAST(warehouse_pallet_stacking_factor AS DECIMAL(18,3)) as warehouse_pallet_stacking_factor  
  from abc_trigger_summary_dim

"""
abc_trigger_summary_dim = spark.sql(abc_trigger_summary_dim)
(abc_trigger_summary_dim.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url",url )
  .option("maxStrLength","4000")
  .option("dbtable","abc_trigger_summary_dim")
  .option("supplier_holiding_period","supplier_holiding_period int") 
  .option("useAzureMSI", "true")
  .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
  .option("user",jdbcUsername)
  .option("password",jdbcPassword)
  .save()
)

# COMMAND ----------

abc_quotation_fam_fact = """
  select 
    right(concat('0000000000', vendor_id), 10) as vendor_id
    ,right(concat('0000', plant_code), 4) as plant_code
    ,abc_mat_fam_new_msm
    ,base_unit_of_measure
    --,qty_forecast_n12
    ,cast(qty_forecast_n12_family as decimal(12,2)) as qty_forecast_n12_family
    ,abc_segment
    ,cast(freight_factor as decimal(12,2)) as freight_factor
    ,flag_supplier_inv_storage
    ,cast(price_unit_factor as int) as price_unit_factor
    ,doc_curr_code
    ,cast(min_prod_qty_sc1 as decimal(12,2)) as min_prod_qty_sc1
    ,cast(runs_annual_sc1 as decimal(12,2)) as runs_annual_sc1
    ,cast(net_price_sc1 as decimal(12,2)) as net_price_sc1
    ,cast(min_prod_qty_sc2 as decimal(12,2)) as min_prod_qty_sc2
    ,cast(runs_annual_sc2 as decimal(12,2)) as runs_annual_sc2
    ,cast(net_price_sc2 as decimal(12,2)) as net_price_sc2
    ,cast(min_prod_qty_sc3 as decimal(12,2)) as min_prod_qty_sc3
    ,cast(runs_annual_sc3 as decimal(12,2)) as runs_annual_sc3
    ,cast(net_price_sc3 as decimal(12,2)) as net_price_sc3
    ,cast(min_prod_qty_sc4 as decimal(12,2)) as min_prod_qty_sc4
    ,cast(runs_annual_sc4 as decimal(12,2)) as runs_annual_sc4
    ,cast(net_price_sc4 as decimal(12,2)) as net_price_sc4
    ,cast(min_prod_qty_sc5 as decimal(12,2)) as min_prod_qty_sc5
    ,cast(runs_annual_sc5 as decimal(12,2)) as runs_annual_sc5
    ,cast(net_price_sc5 as decimal(12,2)) as net_price_sc5
    ,cast(min_prod_qty_sc6 as decimal(12,2)) as min_prod_qty_sc6
    ,cast(runs_annual_sc6 as decimal(12,2)) as runs_annual_sc6
    ,cast(net_price_sc6 as decimal(12,2)) as net_price_sc6
    ,cast(min_prod_qty_sc7 as decimal(12,2)) as min_prod_qty_sc7
    ,cast(runs_annual_sc7 as decimal(12,2)) as runs_annual_sc7
    ,cast(net_price_sc7 as decimal(12,2)) as net_price_sc7
    ,cast(min_prod_qty_sc8 as decimal(12,2)) as min_prod_qty_sc8
    ,cast(runs_annual_sc8 as decimal(12,2)) as runs_annual_sc8
    ,cast(net_price_sc8 as decimal(12,2)) as net_price_sc8
    ,ticket_id
  from abc_quotation_fam_fact
  where abc_mat_fam_new_msm not like '' and abc_mat_fam_new_msm not like ' '
"""
abc_quotation_fam_fact = spark.sql(abc_quotation_fam_fact)


(abc_quotation_fam_fact.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url",url )
  .option("maxStrLength","4000")
  .option("dbtable","abc_quotation_fam_fact")
  ##.option("qty_forecast_n12","qty_forecast_n12 decimal(12,2)") 
  .option("qty_forecast_n12_family","qty_forecast_n12_family decimal(12,2)")
  .option("freight_factor","freight_factor decimal(12,2)")
  .option("price_unit_factor","price_unit_factor int")
  .option("min_prod_qty_sc1","min_prod_qty_sc1 decimal(12,2)")
  .option("runs_annual_sc1","runs_annual_sc1 decimal(12,2)")
  .option("net_price_sc1","net_price_sc1 decimal(12,2)")
  .option("min_prod_qty_sc2","min_prod_qty_sc2 decimal(12,2)")
  .option("runs_annual_sc2","runs_annual_sc2 decimal(12,2)")
  .option("net_price_sc2","net_price_sc2 decimal(12,2)")
  .option("min_prod_qty_sc3","min_prod_qty_sc3 decimal(12,2)")
  .option("runs_annual_sc3","runs_annual_sc3 decimal(12,2)")
  .option("net_price_sc3","net_price_sc3 decimal(12,2)")
  .option("min_prod_qty_sc4","min_prod_qty_sc4 decimal(12,2)")
  .option("runs_annual_sc4","runs_annual_sc4 decimal(12,2)")
  .option("net_price_sc4","net_price_sc4 decimal(12,2)")
  .option("min_prod_qty_sc5","min_prod_qty_sc5 decimal(12,2)")
  .option("runs_annual_sc5","runs_annual_sc5 decimal(12,2)")
  .option("net_price_sc5","net_price_sc5 decimal(12,2)")
  .option("min_prod_qty_sc6","min_prod_qty_sc6 decimal(12,2)")
  .option("runs_annual_sc6","runs_annual_sc6 decimal(12,2)")
  .option("net_price_sc6","net_price_sc6 decimal(12,2)")
  .option("min_prod_qty_sc7","min_prod_qty_sc7 decimal(12,2)")
  .option("runs_annual_sc7","runs_annual_sc7 decimal(12,2)")
  .option("net_price_sc7","net_price_sc7 decimal(12,2)")
  .option("min_prod_qty_sc8","min_prod_qty_sc8 decimal(12,2)")
  .option("runs_annual_sc8","runs_annual_sc8 decimal(12,2)")
  .option("net_price_sc8","net_price_sc8 decimal(12,2)")
  .option("useAzureMSI", "true")
  .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
  .option("user",jdbcUsername)
  .option("password",jdbcPassword)
  .save()
)

# COMMAND ----------

abc_quotation_fact = """

  select 
    right(concat('0000000000', vendor_id), 10) as vendor_id
    ,right(concat('0000', plant_code), 4) as plant_code
    ,right(concat('000000000000000000', material_id), 18) as material_id
    ,abc_mat_fam_new_msm
    ,base_unit_of_measure
    ,cast(qty_forecast_n12 as decimal(12,2)) as qty_forecast_n12
    ,cast(qty_forecast_n12_family as decimal(12,2)) as qty_forecast_n12_family
    ,abc_segment
    ,cast(freight_factor as decimal(12,2)) as freight_factor
    ,flag_supplier_inv_storage
    ,cast(price_unit_factor as int) as price_unit_factor
    ,doc_curr_code
    ,cast(min_prod_qty_sc1 as decimal(12,2)) as min_prod_qty_sc1
    ,cast(runs_annual_sc1 as decimal(12,2)) as runs_annual_sc1
    ,cast(net_price_sc1 as decimal(12,2)) as net_price_sc1
    ,cast(min_prod_qty_sc2 as decimal(12,2)) as min_prod_qty_sc2
    ,cast(runs_annual_sc2 as decimal(12,2)) as runs_annual_sc2
    ,cast(net_price_sc2 as decimal(12,2)) as net_price_sc2
    ,cast(min_prod_qty_sc3 as decimal(12,2)) as min_prod_qty_sc3
    ,cast(runs_annual_sc3 as decimal(12,2)) as runs_annual_sc3
    ,cast(net_price_sc3 as decimal(12,2)) as net_price_sc3
    ,cast(min_prod_qty_sc4 as decimal(12,2)) as min_prod_qty_sc4
    ,cast(runs_annual_sc4 as decimal(12,2)) as runs_annual_sc4
    ,cast(net_price_sc4 as decimal(12,2)) as net_price_sc4
    ,cast(min_prod_qty_sc5 as decimal(12,2)) as min_prod_qty_sc5
    ,cast(runs_annual_sc5 as decimal(12,2)) as runs_annual_sc5
    ,cast(net_price_sc5 as decimal(12,2)) as net_price_sc5
    ,cast(min_prod_qty_sc6 as decimal(12,2)) as min_prod_qty_sc6
    ,cast(runs_annual_sc6 as decimal(12,2)) as runs_annual_sc6
    ,cast(net_price_sc6 as decimal(12,2)) as net_price_sc6
    ,cast(min_prod_qty_sc7 as decimal(12,2)) as min_prod_qty_sc7
    ,cast(runs_annual_sc7 as decimal(12,2)) as runs_annual_sc7
    ,cast(net_price_sc7 as decimal(12,2)) as net_price_sc7
    ,cast(min_prod_qty_sc8 as decimal(12,2)) as min_prod_qty_sc8
    ,cast(runs_annual_sc8 as decimal(12,2)) as runs_annual_sc8
    ,cast(net_price_sc8 as decimal(12,2)) as net_price_sc8
    ,ticket_id
  from abc_quotation_fact
  where material_id not like ' ' or material_id not like ''
"""
abc_quotation_fact = spark.sql(abc_quotation_fact)

(abc_quotation_fact.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url",url )
  .option("maxStrLength","4000")
  .option("dbtable","abc_quotation_fact")
  .option("qty_forecast_n12", "qty_forecast_n12 decimal(12,2)")
  .option("qty_forecast_n12_family", "qty_forecast_n12_family decimal(12,2)")
  .option("freight_factor", "freight_factor decimal(12,2)")
  .option("price_unit_factor", "price_unit_factor int")
  .option("min_prod_qty_sc1", "min_prod_qty_sc1 decimal(12,2)")
  .option("runs_annual_sc1", "runs_annual_sc1 decimal(12,2)")
  .option("net_price_sc1", "net_price_sc1 decimal(12,2)")
  .option("min_prod_qty_sc2", "min_prod_qty_sc2 decimal(12,2)")
  .option("runs_annual_sc2", "runs_annual_sc2 decimal(12,2)")
  .option("net_price_sc2", "net_price_sc2 decimal(12,2)")
  .option("min_prod_qty_sc3", "min_prod_qty_sc3 decimal(12,2)")
  .option("runs_annual_sc3", "runs_annual_sc3 decimal(12,2)")
  .option("net_price_sc3", "net_price_sc3 decimal(12,2)")
  .option("min_prod_qty_sc4", "min_prod_qty_sc4 decimal(12,2)")
  .option("runs_annual_sc4", "runs_annual_sc4 decimal(12,2)")
  .option("net_price_sc4", "net_price_sc4 decimal(12,2)")
  .option("min_prod_qty_sc5", "min_prod_qty_sc5 decimal(12,2)")
  .option("runs_annual_sc5", "runs_annual_sc5 decimal(12,2)")
  .option("net_price_sc5", "net_price_sc5 decimal(12,2)")
  .option("min_prod_qty_sc6", "min_prod_qty_sc6 decimal(12,2)")
  .option("runs_annual_sc6", "runs_annual_sc6 decimal(12,2)")
  .option("net_price_sc6", "net_price_sc6 decimal(12,2)")
  .option("min_prod_qty_sc7", "min_prod_qty_sc7 decimal(12,2)")
  .option("runs_annual_sc7", "runs_annual_sc7 decimal(12,2)")
  .option("net_price_sc7", "net_price_sc7 decimal(12,2)")
  .option("min_prod_qty_sc8", "min_prod_qty_sc8 decimal(12,2)")
  .option("runs_annual_sc8", "runs_annual_sc8 decimal(12,2)")
  .option("net_price_sc8", "net_price_sc8 decimal(12,2)") 
  .option("useAzureMSI", "true")
  .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
  .option("user",jdbcUsername)
  .option("password",jdbcPassword)
  .save()
)


# COMMAND ----------

abc_quotation_summary_dim = """

  select 
    right(concat('0000000000', vendor_id), 10) as vendor_id
    ,ticket_id
    ,modified_date
    ,ticket_owner
    ,buyer_email
    ,cast(quotation_scenario_type as int) as quotation_scenario_type
    ,ticket_custom_id
    ,supplier_input_ticket_id
    ,ticket_title
    ,ticket_status
  from abc_quotation_summary_dim 

"""
abc_quotation_summary_dim = spark.sql(abc_quotation_summary_dim)

(abc_quotation_summary_dim.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url",url )
  .option("maxStrLength","4000")
  .option("dbtable","abc_quotation_summary_dim")
  .option("quotation_scenario_type", "quotation_scenario_type int")
 .option("ticket_custom_id", "ticket_custom_id int")
  .option("useAzureMSI", "true")
  .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
  .option("user",jdbcUsername)
  .option("password",jdbcPassword)
  .save()
)

# COMMAND ----------

abc_supplier_input_dim = """

select 
    right(concat('0000000000', vendor_id), 10) as vendor_id
    ,right(concat('0000', plant_code), 4) as plant_code
    ,right(concat('000000000000000000', material_id), 18) as material_id
    ,cast(qty_forecast_n12 as decimal(10,2)) as qty_forecast_n12
    ,cast(mm_min_lot_size as decimal(10,2)) as mm_min_lot_size
    ,cast(min_prod_qty as decimal(10,2)) as min_prod_qty
    ,CASE WHEN ISNULL(abc_mat_fam_name) THEN right(concat('000000000000000000', material_id), 18) ELSE abc_mat_fam_name END as abc_mat_fam_name
    ,abc_mat_fam_new
    ,abc_mat_fam_new_msm
    ,cast(restriction_min_prod_qty as decimal(10,2)) as restriction_min_prod_qty
    ,cast(restriction_max_prod_qty as decimal(10,2)) as restriction_max_prod_qty
    ,cast(material_per_pallet_qty as decimal(10,2)) as material_per_pallet_qty
    ,cast(material_shelf_life_months as decimal(10,2)) as material_shelf_life_months
    ,pipo_indicator
    ,material_phase_out_phase_in
    ,initiative_name
    ,price_scale_fam_name
    ,supplier_uom
    ,supplier_uom_factor
    ,ticket_id
    ,cast(ifnull(vendor_plant_change_freq,0) as decimal(10,2)) as vendor_plant_change_freq
    ,cast(pallet_stacking_factor as integer)
    ,CAST(prod_rounding_value as INT) as prod_rounding_value
  from abc_supplier_input_dim
  where material_id not like '' or material_id not like ' '
"""
abc_supplier_input_dim = spark.sql(abc_supplier_input_dim)

abc_supplier_input_dim = abc_supplier_input_dim.withColumn("restriction_min_prod_qty",F.col("restriction_min_prod_qty").cast("decimal(10,2)"))\
  .withColumn("restriction_max_prod_qty",F.col("restriction_max_prod_qty").cast("decimal(10,2)"))\
  .withColumn("material_per_pallet_qty",F.col("material_per_pallet_qty").cast("decimal(10,2)"))\
  .withColumn("material_shelf_life_months",F.col("material_shelf_life_months").cast("decimal(10,2)"))\
  .withColumn("min_prod_qty",F.col("min_prod_qty").cast("decimal(10,2)"))\
  .withColumn("qty_forecast_n12",F.col("qty_forecast_n12").cast("decimal(10,2)"))\
  .withColumn("mm_min_lot_size",F.col("mm_min_lot_size").cast("decimal(10,2)"))\
  .withColumn("vendor_plant_change_freq",F.col("vendor_plant_change_freq").cast("decimal(10,2)"))\
  .withColumn("pallet_stacking_factor",F.col("pallet_stacking_factor").cast("integer"))

duplicates_abc_supplier_input_dim = abc_supplier_input_dim.groupby("material_id", "plant_code", "vendor_id", "ticket_id").agg(F.count("*").alias("supplier_input_repeats"))

abc_supplier_input_dim = abc_supplier_input_dim.alias("a").join(duplicates_abc_supplier_input_dim.alias("b"),["material_id", "plant_code", "vendor_id", "ticket_id"],'leftouter').withColumn("supplier_input_repeats",F.when(F.col("supplier_input_repeats").isNull(),F.lit(1)).otherwise(F.col("supplier_input_repeats"))).selectExpr('a.*','supplier_input_repeats') 

abc_supplier_input_dim.coalesce(1).write.mode("overwrite").parquet("/mnt/refined/sbm_data_operation/dbx_tables/abc/abc_supplier_input_dim/")

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
  .option("vendor_plant_change_freq", "vendor_plant_change_freq decimal(10,2)")
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
    ,cast(min_prod_qty as decimal(12,2)) as min_prod_qty
    ,abc_mat_fam_new
    ,ticket_id
    ,pipo_indicator
    ,material_phase_out_phase_in
    ,initiative_name
    ,cast(restriction_min_prod_qty as decimal(12,2)) as restriction_min_prod_qty
    ,cast(material_per_pallet_qty as decimal(12,2)) as material_per_pallet_qty
    ,price_scale_fam_name
  from abc_supplier_input_inprocess_dim 
  where material_id not like '' or material_id not like ' '

"""
abc_supplier_input_inprocess_dim = spark.sql(abc_supplier_input_inprocess_dim)

(abc_supplier_input_inprocess_dim.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url",url )
  .option("maxStrLength","4000")
  .option("dbtable","abc_supplier_input_inprocess_dim")
  .option("min_prod_qty", "min_prod_qty decimal(12,2)")
 .option("restriction_min_prod_qty", "restriction_min_prod_qty decimal(12,2)")
 .option("material_per_pallet_qty", "material_per_pallet_qty decimal(12,2)")
  .option("useAzureMSI", "true")
  .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
  .option("user",jdbcUsername)
  .option("password",jdbcPassword)
  .save()
)

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


