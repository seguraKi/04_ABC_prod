# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook info:
# MAGIC
# MAGIC -  **Business context explanation:**  A table is generated where the user takes the information from the annual forecast to pre-complete the template and based on that he decides if he wants to modify it.
# MAGIC - **Base tables**: 
# MAGIC      - material_regional_dim
# MAGIC      - material_dim
# MAGIC      - material_attr_dim
# MAGIC      - abc_kinaxis_input_fact
# MAGIC      - planning_parameter_fact
# MAGIC      - site_attr_dim
# MAGIC      - material_calc_shelf_life_dim
# MAGIC      - rpm.lead_vendor_fact
# MAGIC      - rpm.material_change_frequency_dim
# MAGIC      - sbm.ivy_prod_agr_dim
# MAGIC      - lookup_sbm_class_map
# MAGIC      - mara_sl
# MAGIC      - spend_pool_hier_dim
# MAGIC      - consumption_fcst_fact
# MAGIC      - bu_comb_dim
# MAGIC - **Developers involved:** Farid Abdullayev
# MAGIC - **Frequency of running:**
# MAGIC   1. Daily
# MAGIC

# COMMAND ----------

# MAGIC %run "../../01_INIT/SBMConfig"

# COMMAND ----------

import datetime

# COMMAND ----------

import sys
from connectors import JDBCConnector
jdbc_connector = JDBCConnector("prod")

from pyspark.sql.functions import col,count
from pyspark.sql.window import Window

# COMMAND ----------

# df = spark.read.parquet("/mnt/sbm_refined/abc_kinaxis_input_fact/")
# df.display()
# df.filter("ticket_id LIKE '%4813596%'").display()

# COMMAND ----------


# view_type = 'jaggaer_view'

# spark.read.parquet("/mnt/refined/material_dim").createOrReplaceGlobalTempView("material_dim")
# spark.read.parquet("/mnt/refined/plant_dim").createOrReplaceGlobalTempView("plant_dim")
# spark.read.parquet("/mnt/refined/geo_dim").createOrReplaceGlobalTempView("geo_dim")

view_type = 'jaggaer_view'
spark.read.parquet("/mnt/mda-prod/material_regional_dim").createOrReplaceGlobalTempView("mara_sl")
spark.read.parquet("/mnt/mda-prod/material_attr_dim").createOrReplaceGlobalTempView("material_attr_dim")
spark.read.parquet("/mnt/mda-prod/site_attr_dim").createOrReplaceGlobalTempView("site_attr_dim")


SBM_Tables_v2.createTable('vw_sbm_material_dim', view_type)
spark.read.parquet("/mnt/sbm_refined/abc_kinaxis_input_fact/").createOrReplaceGlobalTempView("abc_kinaxis_input_fact")
spark.read.parquet("/mnt/mda-prod/bu_comb_dim/").createOrReplaceTempView("bu_comb_dim")

spark.read.parquet("/mnt/mda-prod/planning_parameter_fact").createOrReplaceGlobalTempView("planning_parameter_fact")

sbm_material_dim = spark.sql("select * from global_temp."+"vw_sbm_material_dim")

df_material_calc_shelf_life_dim = spark.read.parquet("/mnt/mda-prod/material_calc_shelf_life_dim")
df_material_calc_shelf_life_dim.createOrReplaceGlobalTempView("material_calc_shelf_life_dim")


abc_supplier_input_dim = spark.read.parquet("/mnt/sbm_refined/abc_supplier_input_dim")
abc_supplier_input_dim.createOrReplaceGlobalTempView("abc_supplier_input_dim")

lead_vendor_fact = spark.read.parquet("/mnt/mda-prod/lead_vendor_fact/")
lead_vendor_fact.createOrReplaceGlobalTempView("lead_vendor_fact")


material_change_frequency = spark.read.parquet("/mnt/mda-prod/material_change_frequency_dim")
material_change_frequency.createOrReplaceGlobalTempView("material_change_frequency_dim")

df_ivy_prod_agr_dim = spark.read.parquet("/mnt/sbm_refined/ivy_prod_agr_dim")
df_ivy_prod_agr_dim.createOrReplaceGlobalTempView("ivy_prod_agr_dim")



spark.read.option("header","true").csv("/mnt/sbm_raw/sharepoint/sbm_class_map").createOrReplaceGlobalTempView("lookup_sbm_class_map")


#SBM_Tables_v2.createTable('vw_sbm_spend_pool_hier_dim', view_type)


consumption_fcst_fact = spark.read.parquet("/mnt/mda-prod/consumption_fcst_fact")
consumption_fcst_fact.createOrReplaceGlobalTempView("consumption_fcst_fact")

# COMMAND ----------

now = datetime.datetime.now()
print(f"Current timestamp: {now}")

# COMMAND ----------

sql = f"""
with shelf_life_temp as (
  SELECT
    material_id,
    source_system_code,
     CASE
       WHEN MIN(calc_shelf_life) = 0 THEN NULL
       ELSE MIN(calc_shelf_life)
     END AS shelf_life
  FROM
    global_temp.material_calc_shelf_life_dim
  GROUP BY
    material_id,
    source_system_code,
    material_type
),
shelf_life as (
  SELECT
    material_id,
    source_system_code,
    case
      when (shelf_life is null)
      or (length(ltrim(rtrim(shelf_life))) = 0)
      or (cast(shelf_life as int) = 0) then ''
      else cast(shelf_life as varchar(10))
    end as material_shelf_life_months
  FROM
    shelf_life_temp
),
temp1 as (
  select
    stcf.*,
    stv.purchase_vendor_id as lead_vendor_id,
    case
      when change_frequency_times like "1"
      or change_frequency_avg_days < 0
      or isnull(change_frequency_avg_days) then 365
      else change_frequency_avg_days
    end as change_frequency_days_avg_adj
  from
    global_temp.material_change_frequency_dim stcf
    inner join global_temp.lead_vendor_fact stv on stcf.plant_code = stv.plant_code
    and stcf.iopt_initiative_pi_material_id = stv.material_id
    and itm_rank = 1
),
change_freq as (
  select
    plant_code,
    lead_vendor_id,
    avg(change_frequency_days_avg_adj) vendor_plant_change_freq
  from
    temp1
  group by
    plant_code,
    lead_vendor_id
),
consumption_fcst_fact12m as (
  select
    plant_code,
    material_id,
    sum(sum_total_requirement_qty) as qty_forecast_n1y
  from
    global_temp.consumption_fcst_fact
  WHERE
    (
      substr(requirement_date_for_the_component, 0, 4) * 100 + substr(requirement_date_for_the_component, 5, 2)
    ) BETWEEN (YEAR(GETDATE()) * 100 + MONTH(GETDATE()))
    AND (
      YEAR(DATEADD(MONTH, 12, GETDATE())) * 100 + MONTH(DATEADD(MONTH, 12, GETDATE()))
    )
    AND material_type in ('ROH', 'HALB')
  group by
    plant_code,
    material_id
)

SELECT *, timestamp'{now}' as timestamp FROM (
select
   distinct
      cast(kif.purchase_vendor_id as int) as vendor_id
      , cast(kif.material_id as int) as material_id
      , md.material_desc
      , concat(pd.plant_code, ': ', pd.plant_name) as plant_code  --Pending Confirm
      , pd.plant_code as only_plant_code                           --Pending Confirm
      , md.base_unit_of_measure
      , kif.business_unit as business_unit                           --Pending Confirm
      , pd.region_override as region                          --Pending Confirm
      , kif.mm_minimum_lot_size as mm_min_lot_size
      , ifnull(kif.min_prod_qty, ipa.min_prod_quantity) as min_prod_qty
      , ifnull(ipa.production_family, kif.abc_mat_fam_name) as abc_mat_fam_name
      , kif.restriction_min_prod_qty
      , kif.restriction_max_prod_qty
      , kif.material_per_pallet_qty
      , sl.material_shelf_life_months
      , kif.spend_pool_low_name as spend_pool_low_name
      , kif.business_unit_lkp_code
      , bu.category as tdc_value
      , kif.pipo_indicator
      , kif.material_phase_out_phase_in
      , kif.iopt_initiative_name as initiative_name
      , cast(kif.prod_rounding_value as int) as prod_rounding_value
      , cast(kif.setup_cost_usd as decimal(10,2)) as set_up_cost
      , FIRST_VALUE(CAST(dim.pallet_stacking_factor as int)) over (
        PARTITION BY kif.material_id, kif.purchase_vendor_id, kif.plant_code
        order by dim.ticket_id desc
      ) as pallet_stacking_factor



      , case 
          when cf.vendor_plant_change_freq is null 
          then 
            (case 
              when sphd.spend_pool_high_name like 'PACKAGING' 
              then 180 
              else 365 
            end) 
          else cf.vendor_plant_change_freq 
        end as vendor_plant_change_freq
from
  global_temp.abc_kinaxis_input_fact kif
  inner join global_temp.vw_sbm_material_dim md on cast(kif.material_id as int) = cast(md.material_number as int)
  left join global_temp.vw_sbm_spend_pool_hier_dim sphd on sphd.spend_pool_lkp_code = md.material_group
  left join bu_comb_dim bu on bu.code = kif.business_unit_lkp_code
  left join change_freq cf on cf.plant_code = kif.plant_code
  and cf.lead_vendor_id = kif.purchase_vendor_id
  left join global_temp.abc_supplier_input_dim dim on dim.material_id = kif.material_id
  AND dim.plant_code = kif.plant_code
  AND dim.vendor_id = purchase_vendor_id
  inner join (
    select
      distinct plant_code,
      plant_name,
      gd.region_override
    from
      global_temp.plant_dim pd
      left join (
        SELECT
          distinct country_code,
          region_name,
          country_name,
          (
            CASE
              WHEN area like '%ASIA PAC%' then 'AMA-E'
              WHEN area like '%CHINA%' then 'GC'
              WHEN area like '%EUROPE%' then 'EUROPE'
              WHEN area like '%IMEA%' then 'AMA-W'
              WHEN area like '%LATIN%' then 'LA'
              WHEN area like '%NORTH%' then 'NA'
              ELSE 'missing'
            END
          ) as region_override
        from
          global_temp.geo_dim
      ) gd on pd.country_code = gd.country_code
  ) pd on pd.plant_code = kif.plant_code
  join shelf_life sl on kif.source_system_code = sl.source_system_code
  and kif.material_id = sl.material_id
  left join global_temp.ivy_prod_agr_dim ipa on CAST(ipa.vendor_id AS INT) = CAST(kif.purchase_vendor_id AS INT)
  and CAST(ipa.material_id AS INT) = cast(kif.material_id as int)

  left join consumption_fcst_fact12m cff12 on cff12.plant_code = pd.plant_code
  and CAST(cff12.material_id AS INT) = cast(kif.material_id as int)
WHERE
   NOT (
    (
      cff12.qty_forecast_n1y <= 0
      AND kif.pipo_indicator is NULL
    )
    OR (
      cff12.qty_forecast_n1y <= 0
      AND kif.pipo_indicator = ''
    )
    OR
    (
      cff12.qty_forecast_n1y is null
      AND (kif.pipo_indicator = '' or kif.pipo_indicator is NULL)
    )
  ))
"""

abc_supplier_template = spark.sql(sql)

# COMMAND ----------

# dublicates = abc_supplier_template.groupBy("vendor_id","material_id","plant_code").count().filter(col("count")>1)
# dublicates.display()

# total_dublicates = dublicates.withColumn("total_dublicates", col('count')-1) \
#                             .agg({"total_dublicates": "sum"}) \
#                             .collect()[0][0]

# total_records = abc_supplier_template.count()

# print(f'Total number of duplicates: {total_dublicates}')
# # print(f'Total number of records: {total_records}')

# COMMAND ----------

# abc_supplier_template.display()
# abc_supplier_template.filter("vendor_id LIKE '%15246580%'").filter("only_plant_code LIKE '%3662%'").display()
# abc_supplier_template.filter("vendor_id =='15246580' AND only_plant_code == '3662' ").display()

# COMMAND ----------

# window_spec = Window.partitionBy("vendor_id","material_id","plant_code")
# table_with_counts = abc_supplier_template.withColumn("count",count("*").over(window_spec))
# duplicated_rows = table_with_counts.filter(col("count")>1).drop("count")

# duplicated_rows.display()

# COMMAND ----------

# try:
#   now_start = datetime.now()
#   title = 'export_jaggaer_view_GenerateCSV'
  
#   #Below code splits the files
#   files = sbm_Function_v2.splitDFtoCSV(abc_supplier_template, "/mnt/temp/sbm/", "abc_supplier_template", 100000)
#   print(files)

#   #Below code SFTP transfers the split files list from above
#   # transfered_files = sbm_Function_v2.transferFiles(files, 'IntoPlatform/abc_supplier_template', JaggaerXferSFTPHost, JaggaerXferSFTPuser, JaggaerXferSFTPpswd, 'put', jaggaer_rsa_key)
#   print(transfered_files)

#   sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start)
# except Exception as ex:
#   sbm_Function_v2.sendMessageWithDF("", "Error found", 'Error found: </br></br> Error generating and uploading the abc_supplier_template csv file...</br>'+title+ '</br>'+NotebookName+'. </br></br>'+str(ex), title )
#   pythonCustomExceptionHandler(ex, "Error generating and uploading the abc_supplier_template csv file...")  
#   sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

from datetime import datetime
from connectors import JaggaerConnector
from pathlib import Path

files = sbm_Function_v2.splitDFtoCSV(abc_supplier_template, "/mnt/sbm_stage/sbm/", "abc_supplier_template", 100000)
jaggaer_connector = JaggaerConnector("prod")
#There is a need for multiple folders with the same data.
remotapath_parent_list = ["IntoPlatform/abc_supplier_template","/IntoPlatform/abc_supplier_template_nofam", "/IntoPlatform/abc_supplier_template_ps_fam", "/IntoPlatform/abc_supplier_template_ps_nofam","/IntoPlatform/abc_supplier_template_cost_nofam","/IntoPlatform/abc_supplier_template_cost_fam"]



list_of_localpaths = files
for remotepath_parent in remotapath_parent_list:
  file_name = Path(remotepath_parent).name
  i=1
  for f in list_of_localpaths:
    path_to_file = Path(f)
    jaggaer_connector.put_file(str(path_to_file), remotepath_parent+"/"+file_name+"_"+str(i)+".csv")
    i=i+1
for f in list_of_localpaths:
  os.rename(str(f), str(f)[:-4] + datetime.today().strftime('_%Y_%m_%d_%H_%M'))

# COMMAND ----------

#sbm_Function_v2.saveTable('abc_supplier_template', 'parquet', 'overwrite', abc_supplier_template)
abc_supplier_template.write.format("parquet").option("header","true").mode("overwrite").save("/mnt/sbm_refined/abc_supplier_template")
# abc_supplier_template.write.format("parquet").option("header","true").mode("overwrite").save("/mnt/sppo-refined-dev/abc_supplier_template")

# COMMAND ----------

dbutils.notebook.exit(notebookReturnSuccess)
