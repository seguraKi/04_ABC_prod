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

path_to_databricks_created_libraries = "/Workspace//prod/01_INIT"
sys.path.append(path_to_databricks_created_libraries)
import sbm_Function_v2
import datetime

# COMMAND ----------

from jaggaer_connectors import JaggaerConnector
from pathlib import Path
from pyspark.sql import Row
#GoAnyWhere
jaggaer_connector = JaggaerConnector(rtEnvironment.lower())
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


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
consumption_fcst_fact = spark.read.parquet("/mnt/mda-prod/consumption_fcst_fact/")
site_attr_dim = spark.read.parquet("/mnt/mda-prod/site_attr_dim")
material_attr_dim = spark.read.parquet("/mnt/mda-prod/material_attr_dim")
bu_comb_dim = spark.read.parquet("/mnt/mda-prod/bu_comb_dim/")
# spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y'").where("net_price_rate" <> 0).groupby("purchase_vendor_id","material_id","plant_code","base_uom","business_unit","material_desc").count().createOrReplaceTempView("active_agreement_fact")
active_agreement_fact = spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y' AND valid_end_date >= date_format(now(),'yyyyMM01') and material_type in ('ROH','HALB')")

############# jaggaer_view ##############################
view_type = 'jaggaer_view'
# SBM_Tables_v2.createTable('vw_sbm_material_dim', view_type)
# SBM_Tables_v2.createTable('vw_sbm_plant_dim', view_type)
vw_sbm_plant_dim = spark.sql(f"select * from global_temp.vw_sbm_plant_dim")
abc_kinaxis_input_fact = spark.read.parquet("/mnt/mda-prod/abc_kinaxis_input_fact/")

# COMMAND ----------

#spark.read.parquet("/mnt/mda-prod/bu_comb_dim/").display()

# COMMAND ----------

#spark.read.parquet("/mnt/mda-prod/consumption_fcst_fact/").display()

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

 
current_year_month = F.year(F.current_date()) * 100 + F.month(F.current_date())
next_year_month = F.year(F.date_add(F.current_date(), 365)) * 100 + F.month(F.date_add(F.current_date(), 365))


consumption_fcst_fact12m = (consumption_fcst_fact
    .filter(
        (F.substring("requirement_date_for_the_component", 1, 4).cast("int") * 100 + 
         F.substring("requirement_date_for_the_component", 6, 2).cast("int") >= current_year_month) &
        (F.substring("requirement_date_for_the_component", 1, 4).cast("int") * 100 + 
         F.substring("requirement_date_for_the_component", 6, 2).cast("int") <= next_year_month) &
        (F.col("material_type").isin('ROH', 'HALB'))
    )
    .groupBy("purchase_vendor_id", "plant_code", "material_id")
    .agg(F.sum("sum_total_requirement_qty").alias("qty_forecast_n1y"))
)

# Obtener la fecha actual
current_year_month_6m = F.year(F.current_date()) * 100 + F.month(F.current_date())
next_year_month_6m = F.year(F.date_add(F.current_date(), 180)) * 100 + F.month(F.date_add(F.current_date(), 180))

# Filtrado y agregación de 6 meses
consumption_fcst_fact6m  = (consumption_fcst_fact
    .filter(
        (F.substring("requirement_date_for_the_component", 1, 4).cast("int") * 100 + 
         F.substring("requirement_date_for_the_component", 6, 2).cast("int") >= current_year_month_6m) &
        (F.substring("requirement_date_for_the_component", 1, 4).cast("int") * 100 + 
         F.substring("requirement_date_for_the_component", 6, 2).cast("int") <= next_year_month_6m)
    )
    .groupBy("purchase_vendor_id", "plant_code", "material_id")
    .agg(
        F.sum("sum_total_requirement_qty").alias("qty_forecast_n6m"),
        (F.sum("sum_total_requirement_qty") * 2).alias("qty_forecast_n6m_annualized")
    )
)
 
current_year_month_3m = F.year(F.current_date()) * 100 + F.month(F.current_date())
next_year_month_3m = F.year(F.date_add(F.current_date(), 90)) * 100 + F.month(F.date_add(F.current_date(), 90))

# Filtrado y agregación de 3 meses
consumption_fcst_fact3m = (consumption_fcst_fact
    .filter(
        (F.substring("requirement_date_for_the_component", 1, 4).cast("int") * 100 + 
         F.substring("requirement_date_for_the_component", 6, 2).cast("int") >= current_year_month_3m) &
        (F.substring("requirement_date_for_the_component", 1, 4).cast("int") * 100 + 
         F.substring("requirement_date_for_the_component", 6, 2).cast("int") <= next_year_month_3m)
    )
    .groupBy("purchase_vendor_id", "plant_code", "material_id")
    .agg(
        F.sum("sum_total_requirement_qty").alias("qty_forecast_n3m"),
        (F.sum("sum_total_requirement_qty") * 4).alias("qty_forecast_n3m_annualized")
    )
)

# COMMAND ----------

display(active_agreement_fact)

# COMMAND ----------

result = (consumption_fcst_fact12m.alias("cff12")
    .join(consumption_fcst_fact6m.alias("cff6"), 
          (F.col("cff12.purchase_vendor_id") == F.col("cff6.purchase_vendor_id")) & 
          (F.col("cff12.plant_code") == F.col("cff6.plant_code")) & 
          (F.col("cff12.material_id") == F.col("cff6.material_id")), "left")
    .join(consumption_fcst_fact3m.alias("cff3"), 
          (F.col("cff12.purchase_vendor_id") == F.col("cff3.purchase_vendor_id")) & 
          (F.col("cff12.plant_code") == F.col("cff3.plant_code")) & 
          (F.col("cff12.material_id") == F.col("cff3.material_id")), "left")
    .join(vw_sbm_plant_dim.alias("pd"), 
          F.col("pd.plant_code") == F.col("cff12.plant_code"), "left")
    .join(active_agreement_fact.alias("aaf"), 
          (F.col("aaf.plant_code") == F.col("cff12.plant_code")) & 
          (F.col("aaf.material_id").cast("int") == F.col("cff12.material_id").cast("int")) & 
          (F.col("aaf.purchase_vendor_id").cast("int") == F.col("cff12.purchase_vendor_id").cast("int")), "left")
    .join(abc_kinaxis_input_fact.alias("kif"), 
          (F.col("cff12.purchase_vendor_id") == F.col("kif.purchase_vendor_id")) & 
          (F.col("cff12.plant_code") == F.col("kif.plant_code")) & 
          (F.col("cff12.material_id") == F.col("kif.material_id")), "left")
    .join(bu_comb_dim.alias("bu"), 
          F.col("bu.code") == F.col("kif.business_unit_lkp_code"), "left")
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import datetime

# Crear una sesión de Spark
spark = SparkSession.builder.getOrCreate()

# Obtener la fecha y hora actual
now = datetime.datetime.now()

# Crear el DataFrame final
final_result = (result
    .filter(
      ~(
        (F.col('cff12.qty_forecast_n1y').cast('float') <= 0) & (F.col('kif.pipo_indicator').isNull()) |
        (F.col('cff12.qty_forecast_n1y').cast('float') <= 0) & (F.col('kif.pipo_indicator') == '') |
        (F.col('cff12.qty_forecast_n1y').isNull() & (F.col('kif.pipo_indicator').isNull() | (F.col('kif.pipo_indicator') == '')))
      )
    )
    .select(
        F.col("cff12.purchase_vendor_id").alias("vendor_id"),  # Renombrado para evitar conflictos
        F.col("aaf.purchase_vendor_id").cast("int").alias("purchase_vendor_id"),
        F.concat(F.col("pd.plant_code"), F.lit(": "), F.col("pd.plant_name")).alias("plant_code"),
        "pd.plant_name",
        F.col("cff12.material_id").cast("int").alias("material_id"),
        "aaf.material_desc",
        F.when(F.col("aaf.base_uom").isNull(), '').otherwise(F.col("aaf.base_uom")).alias("base_unit_of_measure"),
        F.when(F.col("aaf.business_unit").isNull(), '').otherwise(F.col("aaf.business_unit")).alias("business_unit"),
        "cff12.qty_forecast_n1y",
         F.col("pd.plant_code").alias("only_plant_code"),
        "pd.prev_region_name",
        "cff6.qty_forecast_n6m",
        "cff6.qty_forecast_n6m_annualized",
        "cff3.qty_forecast_n3m",
        "cff3.qty_forecast_n3m_annualized",
        "aaf.doc_curr_code",
        F.col("aaf.price_unit_factor").cast("decimal(10,2)").alias("price_unit_factor"),
        F.col("aaf.net_price_rate").cast("decimal(10,2)").alias("net_price_rate"),
        "aaf.spend_pool_low_name",
        "aaf.business_unit_lkp_code",
        "bu.category",
        F.lit(now).alias("date_time"),  # Asegúrate de usar F.lit para la fecha
        "kif.pipo_indicator",
        "kif.material_phase_out_phase_in",
        "kif.iopt_initiative_name"
    )
)


# COMMAND ----------

display(final_result.count())
#121 424 in old version

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
    , aaf.doc_curr_code
    , cast(aaf.price_unit_factor as decimal(10,2)) as price_unit_factor
    , cast(aaf.net_price_rate as decimal(10,2)) as net_price_rate
    , aaf.spend_pool_low_name
    , aaf.business_unit_lkp_code
    , bu.category as tdc_value
    , timestamp'{now}' as date_time
    , kif.pipo_indicator
    , kif.material_phase_out_phase_in
    , kif.iopt_initiative_name
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

#GoAnyWhere
from datetime import datetime
files = sbm_Function_v2.splitDFtoCSV(abc_fcst_override_template, "/mnt/sbm_stage/sbm/", "abc_fcst_override_template", 100000)
#jaggaer_connector = JaggaerConnector("prod")
remotepath_parent = 'IntoPlatform/abc_fcst_override_template' if rtEnvironment == "Prod" else 'pg_demo/IntoPlatform/abc_fcst_override_template'
list_of_localpaths = files
for f in list_of_localpaths:
  path_to_file = Path(f)
  file_name = path_to_file.name
  jaggaer_connector.put_file(str(path_to_file), remotepath_parent+"/"+file_name)
  os.rename(str(f), str(f)[:-4] + datetime.today().strftime('_%Y_%m_%d_%H_%M'))

# COMMAND ----------

dbutils.notebook.exit(notebookReturnSuccess)
