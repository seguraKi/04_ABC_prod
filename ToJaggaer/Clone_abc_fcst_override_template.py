# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook info:
# MAGIC
# MAGIC -  **Business context explanation:**  A table is generated where we have information related to the minimum lot size, mpq, production families, material per pallet, initiatives, etc. to avoid manual entry by the user in the sbm template.
# MAGIC - **Base tables**: 
# MAGIC      - consumption_fcst_fact
# MAGIC      - material_dim
# MAGIC      - site_attr_dim
# MAGIC      - active_agreement_fact
# MAGIC      - abc_kinaxis_input_fact
# MAGIC      - bu_comb_dim
# MAGIC - **Developers involved:** Kenneth Aguilar
# MAGIC - **Frequency of running:**
# MAGIC   1. Daily
# MAGIC

# COMMAND ----------

# MAGIC %run "../../01_INIT/SBMConfig"

# COMMAND ----------

from connectors import JaggaerConnector
from pathlib import Path
import datetime

# COMMAND ----------

# spark.read.parquet("/mnt/mda-prod/consumption_fcst_fact/").createOrReplaceGlobalTempView(TablePrefix+"consumption_fcst_fact")
# spark.read.parquet("/mnt/mda-prod/geo_dim").createOrReplaceGlobalTempView(TablePrefix+"geo_dim")
# spark.read.parquet("/mnt/mda-prod/material_dim").createOrReplaceGlobalTempView(TablePrefix+"material_dim")
# spark.read.parquet("/mnt/mda-prod/plant_dim").createOrReplaceGlobalTempView(TablePrefix+"plant_dim")
# spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y'").groupby("purchase_vendor_id","material_id","plant_code","base_uom","business_unit","material_desc").count().createOrReplaceTempView("active_agreement_fact")
#__________________________________________
# spark.read.parquet("/mnt/mda-prod/consumption_fcst_fact/").createOrReplaceGlobalTempView(TablePrefix+"consumption_fcst_fact")
# spark.read.parquet("/mnt/mda-prod/site_attr_dim").createOrReplaceGlobalTempView(TablePrefix+"geo_dim")
# spark.read.parquet("/mnt/mda-prod/material_attr_dim").createOrReplaceGlobalTempView(TablePrefix+"material_dim")
# spark.read.parquet("/mnt/mda-prod/site_attr_dim").createOrReplaceGlobalTempView(TablePrefix+"plant_dim")
# spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y'").groupby("purchase_vendor_id","material_id","plant_code","base_uom","business_unit","material_desc").count().createOrReplaceTempView("active_agreement_fact")
#_____________________________________________________
spark.read.parquet("/mnt/mda-prod/consumption_fcst_fact/").createOrReplaceGlobalTempView(TablePrefix+"consumption_fcst_fact")
spark.read.parquet("/mnt/mda-prod/site_attr_dim").createOrReplaceGlobalTempView(TablePrefix+"site_attr_dim")
spark.read.parquet("/mnt/mda-prod/material_attr_dim").createOrReplaceGlobalTempView(TablePrefix+"material_attr_dim")
spark.read.parquet("/mnt/mda-prod/bu_comb_dim/").createOrReplaceTempView("bu_comb_dim")
# spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y'").where("net_price_rate" <> 0).groupby("purchase_vendor_id","material_id","plant_code","base_uom","business_unit","material_desc").count().createOrReplaceTempView("active_agreement_fact")
spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y'").groupby("purchase_vendor_id","material_id","plant_code","base_uom","business_unit","material_desc","doc_curr_code","price_unit_factor","spend_pool_low_name","business_unit_lkp_code","net_price_rate").count().createOrReplaceTempView("active_agreement_fact")

############# jaggaer_view ##############################
view_type = 'jaggaer_view'
# SBM_Tables_v2.createTable('vw_sbm_material_dim', view_type)
# SBM_Tables_v2.createTable('vw_sbm_plant_dim', view_type)
spark.read.parquet("/mnt/mda-prod/abc_kinaxis_input_fact/").createOrReplaceTempView("abc_kinaxis_input_fact")

# COMMAND ----------

#spark.read.parquet("/mnt/mda-prod/bu_comb_dim/").display()

# COMMAND ----------

#spark.read.parquet("/mnt/mda-prod/consumption_fcst_fact/").display()

# COMMAND ----------

#spark.read.parquet("/mnt/mda-prod/active_agreement_fact").display()

# COMMAND ----------

#spark.read.parquet("/mnt/mda-prod/abc_kinaxis_input_fact/").display()

# COMMAND ----------

#spark.read.parquet("/mnt/mda-prod/consumption_fcst_fact/").display()

# COMMAND ----------

#spark.read.parquet("/mnt/mda-prod/site_attr_dim").display()

# COMMAND ----------

#spark.read.parquet("/mnt/mda-prod/material_attr_dim").display()

# COMMAND ----------

#spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y'").groupby("purchase_vendor_id","material_id","plant_code","base_uom","business_unit","material_desc","doc_curr_code","price_unit_factor","spend_pool_low_name","business_unit_lkp_code","net_price_rate").count().display()

# COMMAND ----------

now = datetime.datetime.now()
print(f"Current timestamp: {now}")

# COMMAND ----------

sql = f"""

  with consumption_fcst_fact12m as (
    select 
      purchase_vendor_id as vendor_id
      ,plant_code
      ,material_id
      ,sum(sum_total_requirement_qty) as qty_forecast_n1y
    from global_temp.consumption_fcst_fact
     WHERE
    (
      substr(requirement_date_for_the_component, 0, 4) * 100 + substr(requirement_date_for_the_component, 5, 2)
    ) BETWEEN (YEAR(GETDATE()) * 100 + MONTH(GETDATE()))
    AND (
      YEAR(DATEADD(MONTH, 12, GETDATE())) * 100 + MONTH(DATEADD(MONTH, 12, GETDATE()))
    )
    AND material_type in ('ROH', 'HALB')
    group by 
      purchase_vendor_id
      ,plant_code
      ,material_id
  ),

  consumption_fcst_fact6m as (
    select 
      purchase_vendor_id as vendor_id
      ,plant_code
      ,material_id
      ,sum(sum_total_requirement_qty) as qty_forecast_n6m
      ,sum(sum_total_requirement_qty)*2 as qty_forecast_n6m_annualized
    from global_temp.consumption_fcst_fact
    WHERE (substr(requirement_date_for_the_component,0,4) * 100 + substr(requirement_date_for_the_component,5,2)) BETWEEN (YEAR(GETDATE()) * 100 + MONTH(GETDATE())) AND (YEAR(DATEADD(MONTH, 6, GETDATE())) * 100 + MONTH(DATEADD(MONTH, 6, GETDATE())))
    group by 
      purchase_vendor_id
      ,plant_code
      ,material_id
  ),

  consumption_fcst_fact3m as (
    select 
      purchase_vendor_id as vendor_id
      ,plant_code
      ,material_id
      ,sum(sum_total_requirement_qty) as qty_forecast_n3m
      ,sum(sum_total_requirement_qty)*4 as qty_forecast_n3m_annualized
    from global_temp.consumption_fcst_fact
    WHERE (substr(requirement_date_for_the_component,0,4) * 100 + substr(requirement_date_for_the_component,5,2)) BETWEEN (YEAR(GETDATE()) * 100 + MONTH(GETDATE())) AND (YEAR(DATEADD(MONTH, 3, GETDATE())) * 100 + MONTH(DATEADD(MONTH, 3, GETDATE())))
    group by 
      purchase_vendor_id
      ,plant_code
      ,material_id
  )

  
  select 
    cff12.vendor_id
    ,cast(aaf.purchase_vendor_id as int)
    ,concat(pd.plant_code, ': ', pd.plant_name) as plant_code
    , pd.plant_name
    , cast(cff12.material_id as int) as material_id
    , aaf.material_desc
    , ifnull(aaf.base_uom, '') as base_unit_of_measure
    , ifnull(aaf.business_unit, '') as business_unit
    , cff12.qty_forecast_n1y
    , pd.plant_code as only_plant_code
    , pd.prev_region_name as region
    , cff6.qty_forecast_n6m
    , cff6.qty_forecast_n6m_annualized
    , cff3.qty_forecast_n3m
    , cff3.qty_forecast_n3m_annualized
    , aaf.doc_curr_code as doc_curr_code_override
    , cast(aaf.price_unit_factor as decimal(10,2)) as price_unit_factor_override
    , cast(aaf.net_price_rate as decimal(10,2)) as net_price_rate_override
    , aaf.spend_pool_low_name
    , aaf.business_unit_lkp_code
    , bu.category as tdc_value
    , timestamp'{now}' as date_time
  from consumption_fcst_fact12m cff12
  left join consumption_fcst_fact6m cff6
    on cff12.vendor_id = cff6.vendor_id
    and cff12.plant_code = cff6.plant_code
    and cff12.material_id = cff6.material_id
  left join consumption_fcst_fact3m cff3
    on cff12.vendor_id = cff3.vendor_id
    and cff12.plant_code = cff3.plant_code
    and cff12.material_id = cff3.material_id
  left join global_temp.vw_sbm_plant_dim pd
    on pd.plant_code = cff12.plant_code
  left join active_agreement_fact aaf
    on aaf.plant_code = cff12.plant_code
    and cast(aaf.material_id as int) = cast(cff12.material_id as int)
    and cast(aaf.purchase_vendor_id as int) = cast(cff12.vendor_id as int)
  left join abc_kinaxis_input_fact kif
  on cff12.vendor_id = kif.purchase_vendor_id
    and cff12.plant_code = kif.plant_code
    and cff12.material_id = kif.material_id 
 left join bu_comb_dim bu
 on bu.code = kif.business_unit_lkp_code
 WHERE
    NOT (

     (
      cff12.qty_forecast_n1y <= 0
      AND pipo_indicator is NULL
    )
    OR (
      cff12.qty_forecast_n1y <= 0
      AND pipo_indicator = ''
    )
    OR
    (
      cff12.qty_forecast_n1y is null
      AND (pipo_indicator = '' or pipo_indicator is NULL)
    )
  )


"""
abc_fcst_override_template = spark.sql(sql) 

# COMMAND ----------

#abc_fcst_override_template.display()

# COMMAND ----------

#GoAnyWhere
from datetime import datetime
files = sbm_Function_v2.splitDFtoCSV(abc_fcst_override_template, "/mnt/sbm_stage/sbm/", "abc_fcst_override_template", 100000)
jaggaer_connector = JaggaerConnector("prod")
remotepath_parent = 'pg_demo/IntoPlatform/abc_fcst_override_template'
list_of_localpaths = files
for f in list_of_localpaths:
  path_to_file = Path(f)
  file_name = path_to_file.name
  jaggaer_connector.put_file(str(path_to_file), remotepath_parent+"/"+file_name)
  os.rename(str(f), str(f)[:-4] + datetime.today().strftime('_%Y_%m_%d_%H_%M'))

# COMMAND ----------

dbutils.notebook.exit(notebookReturnSuccess)
