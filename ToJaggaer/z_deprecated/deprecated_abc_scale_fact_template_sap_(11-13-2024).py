# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook info:
# MAGIC
# MAGIC -  **Business context explanation:**  A table is generated where the information from the price scales from SAP is saved to be used in other analyzes to avoid rework for the user.
# MAGIC - **Base tables**: 
# MAGIC      - price_scale_dim
# MAGIC      - price_validity_dim
# MAGIC      - active_agreement_fact
# MAGIC - **Developers involved:** Kenneth Aguilar
# MAGIC - **Frequency of running:**
# MAGIC   1. Daily
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/prod/01_INIT/SBMConfig"

# COMMAND ----------

from connectors import JaggaerConnector
from pathlib import Path
jaggaer_connector = JaggaerConnector("prod")

# COMMAND ----------

# spark.read.parquet('/mnt/mda-prod/price_scale_dim').createOrReplaceTempView('ps')
# spark.read.parquet('/mnt/mda-prod/price_validity_dim').createOrReplaceTempView('pv')
#spark.read.parquet('/mnt/refined/active_agreement_fact').createOrReplaceTempView('aaf')
spark.read.parquet('/mnt/mda-prod/price_scale_dim').createOrReplaceTempView('ps')
spark.read.parquet('/mnt/mda-prod/price_validity_dim').createOrReplaceTempView('pv')
spark.read.parquet('/mnt/mda-prod/active_agreement_fact').createOrReplaceTempView('aaf')

# COMMAND ----------

abc_scale_fact_template_sap = """

with temp_pv as (
  select  
    max(valid_end_date) as valid_end_date,
    source_system_code,
    purchase_doc_num,
    purchase_doc_line_num
  from pv 
  group by 
    source_system_code,
    purchase_doc_num,
    purchase_doc_line_num

),

temp_pvF as (

  select 
    tp.source_system_code,
    tp.purchase_doc_num,
    tp.purchase_doc_line_num,
    pv.cond_rec_num
  from temp_pv tp 
  inner join pv 
    on pv.valid_end_date = tp.valid_end_date 
    and pv.source_system_code = tp.source_system_code
    and pv.purchase_doc_num = tp.purchase_doc_num
    and pv.purchase_doc_line_num = tp.purchase_doc_line_num

)

select distinct 
    cast(aaf.purchase_vendor_id as int) as vendor_id
  , concat(aaf.plant_code, ': ', aaf.plant_name) as plant_code 
  , cast(aaf.material_id as int ) as material_id
  , aaf.order_uom as scale_uom
  , ps.scale_quantity as mpq_scale_qty
  , aaf.doc_curr_code
  , cast(ps.rate as decimal(10,2)) as scale_price
  , aaf.price_unit_factor
  , ps.valid_start_date
  , aaf.plant_code as only_plant_code

  ,aaf.source_system_code
  ,aaf.purchase_doc_num
  ,aaf.purchase_doc_line_num
from aaf
join (
    select 
      ps.scale_quantity 
      ,ps.rate
      ,ps.valid_start_date
      ,ps.source_system_code
      ,ps.purchase_doc_num
      ,ps.purchase_doc_line_num
    from ps 
    inner join temp_pvF pv
      on ps.source_system_code = pv.source_system_code
      and ps.purchase_doc_num = pv.purchase_doc_num
      and ps.purchase_doc_line_num = pv.purchase_doc_line_num
      and cast(ps.cond_rec_num as int) = cast(pv.cond_rec_num as int)
) ps
  on (
    aaf.source_system_code = ps.source_system_code 
    and aaf.purchase_doc_num = ps.purchase_doc_num 
    and aaf.purchase_doc_line_num = ps.purchase_doc_line_num 
  )
WHERE aaf.source_system_code in ('F6PS4H', 'ANP430','L6P430','A6P430','N6P420') AND aaf.source_list_flag='Y'
"""
abc_scale_fact_template_sap = spark.sql(abc_scale_fact_template_sap)

# COMMAND ----------

#GoAnyWhere
files = sbm_Function_v2.splitDFtoCSV(abc_scale_fact_template_sap, "/mnt/sbm_stage/sbm/", "abc_scale_fact_template_sap", 100000)
remotepath_parent = 'IntoPlatform/abc_scale_fact_template_sap'
list_of_localpaths = files
for f in list_of_localpaths:
  path_to_file = Path(f)
  file_name = path_to_file.name
  jaggaer_connector.put_file(str(path_to_file), remotepath_parent+"/"+file_name)
  os.rename(str(f), str(f)[:-4] + datetime.today().strftime('_%Y_%m_%d_%H_%M'))


# COMMAND ----------

dbutils.notebook.exit(notebookReturnSuccess)
