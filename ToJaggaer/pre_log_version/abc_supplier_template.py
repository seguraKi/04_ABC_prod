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



# COMMAND ----------

# MAGIC %run "../../01_INIT/SBMConfig"

# COMMAND ----------

import datetime

# COMMAND ----------

path_to_databricks_created_libraries = "/Workspace//prod/01_INIT"
sys.path.append(path_to_databricks_created_libraries)
import sbm_Function_v2

# COMMAND ----------

from jaggaer_connectors import JaggaerConnector
from pathlib import Path
from pyspark.sql import Row
#GoAnyWhere
jaggaer_connector = JaggaerConnector(rtEnvironment.lower())

# COMMAND ----------


# view_type = 'jaggaer_view'

# spark.read.parquet("/mnt/refined/material_dim").createOrReplaceGlobalTempView("material_dim")
df_plant_dim = spark.sql(f"select * from global_temp.vw_sbm_plant_dim")
df_geo_dim = spark.read.parquet("/mnt/mda-prod/geo_dim")

view_type = 'jaggaer_view'
spark.read.parquet("/mnt/mda-prod/material_regional_dim").createOrReplaceTempView("mara_sl")
spark.read.parquet("/mnt/mda-prod/material_attr_dim").createOrReplaceTempView("material_attr_dim")
spark.read.parquet("/mnt/mda-prod/site_attr_dim").createOrReplaceTempView("site_attr_dim")


sbm_material_dim = spark.sql(f"select * from global_temp.vw_sbm_material_dim")
df_abc_kinaxis_input_fact = spark.read.parquet("/mnt/sbm_refined/abc_kinaxis_input_fact/")
df_bu_comb_dim = spark.read.parquet("/mnt/mda-prod/bu_comb_dim/")

spark.read.parquet("/mnt/mda-prod/planning_parameter_fact").createOrReplaceTempView("planning_parameter_fact")

df_material_calc_shelf_life_dim = spark.read.parquet("/mnt/mda-prod/material_calc_shelf_life_dim")
df_material_calc_shelf_life_dim.createOrReplaceTempView("material_calc_shelf_life_dim")


abc_supplier_input_dim = spark.read.parquet("/mnt/sbm_refined/abc_supplier_input_dim")
abc_supplier_input_dim.createOrReplaceTempView("abc_supplier_input_dim")

lead_vendor_fact = spark.read.parquet("/mnt/mda-prod/lead_vendor_fact/")
lead_vendor_fact.createOrReplaceTempView("lead_vendor_fact")


material_change_frequency = spark.read.parquet("/mnt/mda-prod/material_change_frequency_dim")
material_change_frequency.createOrReplaceTempView("material_change_frequency_dim")

df_ivy_prod_agr_dim = spark.read.parquet("/mnt/sbm_refined/ivy_prod_agr_dim")
df_ivy_prod_agr_dim.createOrReplaceTempView("ivy_prod_agr_dim")

df_active_agr_fact = spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y' AND valid_end_date >= date_format(now(),'yyyyMM01') and material_type in ('ROH','HALB')")
df_active_agr_fact.createOrReplaceTempView("active_agreement_fact")



spark.read.option("header","true").csv("/mnt/sbm_raw/sharepoint/sbm_class_map").createOrReplaceTempView("lookup_sbm_class_map")


sbm_spend_pool_hier_dim = spark.sql(f"select * from global_temp.vw_sbm_spend_pool_hier_dim")


consumption_fcst_fact = spark.read.parquet("/mnt/mda-prod/consumption_fcst_fact")
consumption_fcst_fact.createOrReplaceTempView("consumption_fcst_fact")

# COMMAND ----------

now = datetime.datetime.now()
print(f"Current timestamp: {now}")

# COMMAND ----------

# Paso 1: Crear el DataFrame shelf_life_temp
shelf_life_temp = (
    df_material_calc_shelf_life_dim
    .groupBy("material_id", "source_system_code", "material_type")
    .agg(
        F.when(F.min("calc_shelf_life") == 0, None)
         .otherwise(F.min("calc_shelf_life")).alias("shelf_life")
    )
)

# Paso 2: Crear el DataFrame shelf_life
shelf_life = (
    shelf_life_temp
    .select(
        "material_id",
        "source_system_code",
        F.when(
            (F.col("shelf_life").isNull()) |
            (F.length(F.trim(F.col("shelf_life"))) == 0) |
            (F.col("shelf_life").cast(StringType()) == '0'), 
            ''
        ).otherwise(F.col("shelf_life").cast(StringType())).alias("material_shelf_life_months")
    )
)

# COMMAND ----------

temp1 = (
    material_change_frequency
    .join(lead_vendor_fact, 
          (material_change_frequency.plant_code == lead_vendor_fact.plant_code) &
          (material_change_frequency.iopt_initiative_pi_material_id == lead_vendor_fact.material_id) &
          (lead_vendor_fact.itm_rank == 1), 'inner')
    .select(
        material_change_frequency["*"], 
        lead_vendor_fact.purchase_vendor_id.alias("lead_vendor_id"),
        F.when(
            (material_change_frequency.change_frequency_times.like("1")) |
            (material_change_frequency.change_frequency_avg_days < 0) |
            (material_change_frequency.change_frequency_avg_days.isNull()), 
            365
        ).otherwise(material_change_frequency.change_frequency_avg_days).alias("change_frequency_days_avg_adj")
    )
)
change_freq = (
    temp1
    .groupBy("plant_code", "lead_vendor_id")
    .agg(F.avg("change_frequency_days_avg_adj").alias("vendor_plant_change_freq"))
)

# COMMAND ----------


current_year_month = F.year(F.current_date()) * 100 + F.month(F.current_date())
next_year_month = F.year(F.add_months(F.current_date(), 12)) * 100 + F.month(F.add_months(F.current_date(), 12))

consumption_fcst_fact12m = (
    consumption_fcst_fact 
    .filter(
        (F.substring("requirement_date_for_the_component", 1, 4).cast("int") * 100 + 
          F.substring("requirement_date_for_the_component", 6, 2).cast("int"))
        .between(current_year_month, next_year_month) & 
        (F.col("material_type").isin('ROH', 'HALB'))
    )
    .groupBy("plant_code", "material_id")
    .agg(F.sum("sum_total_requirement_qty").alias("qty_forecast_n1y"))
)


# COMMAND ----------

df_abc_kinaxis_input_fact = df_abc_kinaxis_input_fact.select("purchase_vendor_id","material_id","business_unit","mm_minimum_lot_size","min_prod_qty","restriction_min_prod_qty","restriction_max_prod_qty","material_per_pallet_qty","spend_pool_low_name","business_unit_lkp_code","pipo_indicator","material_phase_out_phase_in","iopt_initiative_name","prod_rounding_value","setup_cost_usd","plant_code","source_system_code","abc_mat_fam_name","qty_forecast_n12")
sbm_material_dim = sbm_material_dim.select("material_number","material_desc","base_unit_of_measure","material_group")
df_plant_dim = df_plant_dim.select("plant_code","plant_name","country_code")
df_ivy_prod_agr_dim = df_ivy_prod_agr_dim.select("vendor_id","material_id","min_prod_quantity","production_family")
abc_supplier_input_dim  = abc_supplier_input_dim.select("ticket_id","material_id","plant_code","vendor_id","pallet_stacking_factor")
sbm_spend_pool_hier_dim = sbm_spend_pool_hier_dim.drop("spend_pool_low_name")


# COMMAND ----------

df_plant_geo_dim = (
    df_plant_dim.alias('pd')
    .join(
        df_geo_dim.alias('gd'),
        F.col('pd.country_code') == F.col('gd.country_code'),
        'left'
    )
    .select(
        'pd.plant_code', 
        'pd.plant_name', 
        F.when(F.col('gd.area').like('%ASIA PAC%'), 'AMA-E')
        .when(F.col('gd.area').like('%CHINA%'), 'GC')
        .when(F.col('gd.area').like('%EUROPE%'), 'EUROPE')
        .when(F.col('gd.area').like('%IMEA%'), 'AMA-W')
        .when(F.col('gd.area').like('%LATIN%'), 'LA')
        .when(F.col('gd.area').like('%NORTH%'), 'NA')
        .otherwise('missing').alias('region_override')
    )
)


# COMMAND ----------

result = (
    df_abc_kinaxis_input_fact.alias('kif')
    .join(
        sbm_material_dim.alias('md'), 
        (F.col('kif.material_id').cast('int') == F.col('md.material_number').cast('int')), 
        'inner'
    )
     .join(sbm_spend_pool_hier_dim.alias('sphd'), 
           F.col('sphd.spend_pool_lkp_code') == F.col('md.material_group'), 
           'left')
     .join(change_freq.alias('cf'), 
            (F.col('cf.plant_code') == F.col('kif.plant_code')) &
            (F.col('cf.lead_vendor_id') == F.col('kif.purchase_vendor_id')), 
           'left')
      .join(df_active_agr_fact.alias('aa'), 
            (F.col('aa.material_id') == F.col('kif.material_id')) & 
            (F.col('aa.plant_code') == F.col('kif.plant_code')) & 
            (F.col('aa.purchase_vendor_id') == F.col('kif.purchase_vendor_id')) & 
            (F.col('aa.purchase_vendor_id') == F.col('kif.purchase_vendor_id')), 
            'left')
     .join(abc_supplier_input_dim.alias('dim'),  #Increase number of rows significantly
           (F.col('dim.material_id') == F.col('kif.material_id')) & 
           (F.col('dim.plant_code') == F.col('kif.plant_code')) & 
           (F.col('dim.vendor_id') == F.col('kif.purchase_vendor_id')), 
           'left')
     .join(df_plant_geo_dim.alias('pd'), 
           F.col('pd.plant_code') == F.col('kif.plant_code'), 
           'left')
     .join(shelf_life.alias('sl'), 
           (F.col('kif.source_system_code') == F.col('sl.source_system_code')) & 
           (F.col('kif.material_id') == F.col('sl.material_id')), 
           'inner')
     .join(
         df_ivy_prod_agr_dim.alias('ipa'), 
         (F.col('ipa.vendor_id').cast('int') == F.col('kif.purchase_vendor_id').cast('int')) & 
         (F.col('ipa.material_id').cast('int') == F.col('kif.material_id').cast('int')), 
         'left'
     )
     .join(df_bu_comb_dim.alias('bu'), 
         F.col('bu.code') == F.col('kif.business_unit_lkp_code'), 
         'left')
     .drop(
        change_freq['plant_code'],
        df_active_agr_fact['material_id'],
        df_active_agr_fact['purchase_vendor_id'],
        df_active_agr_fact['business_unit'],
        df_active_agr_fact['spend_pool_low_name'],
        df_active_agr_fact['business_unit_lkp_code'],
        df_active_agr_fact['plant_code'],
        df_active_agr_fact['source_system_code'],
        df_active_agr_fact['material_desc'],
        df_active_agr_fact['spend_pool_lkp_code'],
        df_active_agr_fact['spend_pool_high_name'],
        df_active_agr_fact['spend_pool_medium_name'],
        df_active_agr_fact['spend_pool_group'],
        abc_supplier_input_dim['plant_code'],
        abc_supplier_input_dim['material_id'],
        df_plant_geo_dim['plant_code'],
        df_plant_geo_dim['plant_name'],
        shelf_life['material_id'],
        shelf_life['source_system_code'],
        df_ivy_prod_agr_dim['material_id'],
        df_ivy_prod_agr_dim['vendor_id'],
        df_bu_comb_dim['business_unit'],
        df_bu_comb_dim['sector_business_unit']
        )
)

# COMMAND ----------

# from pyspark.sql import functions as F
# from pyspark.sql import Window

# # Paso 1: Realizar las uniones
# result = (
#     df_abc_kinaxis_input_fact.alias('kif')
#     .join(
#         sbm_material_dim.alias('md'), 
#         (F.col('kif.material_id').cast('int') == F.col('md.material_number').cast('int')), 
#         'inner'
#     )
#     .join(sbm_spend_pool_hier_dim.alias('sphd'), 
#           F.col('sphd.spend_pool_lkp_code') == F.col('md.material_group'), 
#           'left')
#     .join(df_bu_comb_dim.alias('bu'), 
#           F.col('bu.code') == F.col('kif.business_unit_lkp_code'), 
#           'left')
#     .join(change_freq.alias('cf'), 
#           F.col('cf.plant_code') == F.col('kif.plant_code'), 
#           'left')
#     .join(df_active_agr_fact.alias('aa'), 
#           (F.col('aa.material_id') == F.col('kif.material_id')) & 
#           (F.col('aa.plant_code') == F.col('kif.plant_code')) & 
#           (F.col('aa.purchase_vendor_id') == F.col('kif.purchase_vendor_id')) & 
#           (F.col('cf.lead_vendor_id') == F.col('kif.purchase_vendor_id')), 
#           'left')
#     .join(abc_supplier_input_dim.alias('dim'), 
#           (F.col('dim.material_id') == F.col('kif.material_id')) & 
#           (F.col('dim.plant_code') == F.col('kif.plant_code')) & 
#           (F.col('dim.vendor_id') == F.col('kif.purchase_vendor_id')), 
#           'left')
#     .join(
#         df_plant_dim.alias('pd')
#         .join(
#             df_geo_dim.alias('gd'),
#             F.col('pd.country_code') == F.col('gd.country_code'),
#             'left'
#         ).select(
#             'pd.plant_code', 
#             'pd.plant_name', 
#             F.when(F.col('gd.area').like('%ASIA PAC%'), 'AMA-E')
#             .when(F.col('gd.area').like('%CHINA%'), 'GC')
#             .when(F.col('gd.area').like('%EUROPE%'), 'EUROPE')
#             .when(F.col('gd.area').like('%IMEA%'), 'AMA-W')
#             .when(F.col('gd.area').like('%LATIN%'), 'LA')
#             .when(F.col('gd.area').like('%NORTH%'), 'NA')
#             .otherwise('missing').alias('region_override')
#         ),
#         F.col('pd.plant_code') == F.col('kif.plant_code'),
#         'inner'
#     )
#     .join(shelf_life.alias('sl'), 
#           (F.col('kif.source_system_code') == F.col('sl.source_system_code')) & 
#           (F.col('kif.material_id') == F.col('sl.material_id')), 
#           'inner')
#     .join(
#         df_ivy_prod_agr_dim.alias('ipa'), 
#         (F.col('ipa.vendor_id').cast('int') == F.col('kif.purchase_vendor_id').cast('int')) & 
#         (F.col('ipa.material_id').cast('int') == F.col('kif.material_id').cast('int')), 
#         'left'
#     )
# )


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window


final = (
    result
    .select(
        'kif.*',
        F.col('kif.purchase_vendor_id').cast('int').alias('vendor_id'),
        F.col('kif.material_id').cast('int').alias('material_id'),
        'md.material_desc',
        F.concat(F.col('kif.plant_code'), F.lit(': '), F.col('aa.plant_name')).alias('plant_code'),
        F.col('kif.plant_code').alias('only_plant_code') , 
        'md.base_unit_of_measure',
        F.col('pd.region_override').alias('region') ,
        F.col('kif.mm_minimum_lot_size').alias('mm_min_lot_size'),
        F.coalesce(F.col('kif.min_prod_qty'), F.col('ipa.min_prod_quantity')).alias('min_prod_qty'),
        F.coalesce(F.col('ipa.production_family'), F.col('kif.abc_mat_fam_name')).alias('abc_mat_fam_name'),
        'sl.material_shelf_life_months',
        F.col('bu.category').alias('tdc_value'), 
        F.col('kif.iopt_initiative_name').alias('initiative_name'),
        F.col('kif.prod_rounding_value').cast('int').alias('prod_rounding_value'),
        F.round(F.col('kif.setup_cost_usd'),2),
        F.first(F.col('dim.pallet_stacking_factor').cast('int')).over(
            Window.partitionBy('kif.material_id', 'kif.purchase_vendor_id', 'kif.plant_code').orderBy(F.col('dim.ticket_id').desc())
        ).alias('pallet_stacking_factor'),
        F.when(F.col('cf.vendor_plant_change_freq').isNull(),
             F.when(F.col('sphd.spend_pool_high_name').like('PACKAGING'), 180).otherwise(365)
        ).otherwise(F.col('cf.vendor_plant_change_freq')).alias('vendor_plant_change_freq'),
        'aa.purchase_doc_type_desc',
        F.current_timestamp().alias('timestamp')
    )
).drop(
    df_abc_kinaxis_input_fact["min_prod_qty"],
    df_abc_kinaxis_input_fact["abc_mat_fam_name"],
    df_abc_kinaxis_input_fact["prod_rounding_value"],
    df_abc_kinaxis_input_fact["purchase_vendor_id"],
    df_abc_kinaxis_input_fact["material_id"],
    df_abc_kinaxis_input_fact["plant_code"]
)

test = final
final = final.filter(
    ~(
        (F.col('kif.qty_forecast_n12').cast('float') <= 0) & (F.col('kif.pipo_indicator').isNull()) |
        (F.col('kif.qty_forecast_n12').cast('float') <= 0) & (F.col('kif.pipo_indicator') == '') |
        (F.col('kif.qty_forecast_n12').isNull() & (F.col('kif.pipo_indicator').isNull() | (F.col('kif.pipo_indicator') == '')))
    )
)

final = final.distinct()


# COMMAND ----------

abc_supplier_template = final.select("vendor_id",
"material_id",
"material_desc",
"plant_code",
"only_plant_code",
"base_unit_of_measure",
"business_unit",
"region",
"mm_min_lot_size",
"min_prod_qty",
"abc_mat_fam_name",
"restriction_min_prod_qty",
"restriction_max_prod_qty",
"material_per_pallet_qty",
"material_shelf_life_months",
"spend_pool_low_name",
"business_unit_lkp_code",
"tdc_value",
"pipo_indicator",
"material_phase_out_phase_in",
"initiative_name",
"prod_rounding_value",
"setup_cost_usd",
"pallet_stacking_factor",
"vendor_plant_change_freq",
"purchase_doc_type_desc",
"timestamp"
)

# COMMAND ----------

from datetime import datetime
from connectors import JaggaerConnector
from pathlib import Path

files = sbm_Function_v2.splitDFtoCSV(abc_supplier_template, "/mnt/sbm_stage/sbm/", "abc_supplier_template", 100000)

#There is a need for multiple folders with the same data.
remotapath_parent_list_demo = ["pg_demo/IntoPlatform/abc_supplier_template","pg_demo/IntoPlatform/abc_supplier_template_nofam", "pg_demo/IntoPlatform/abc_supplier_template_ps_fam", "pg_demo/IntoPlatform/abc_supplier_template_ps_nofam","pg_demo/IntoPlatform/abc_supplier_template_cost_nofam","pg_demo/IntoPlatform/abc_supplier_template_cost_fam"]

remotapath_parent_list_prod = ["IntoPlatform/abc_supplier_template","/IntoPlatform/abc_supplier_template_nofam", "/IntoPlatform/abc_supplier_template_ps_fam", "/IntoPlatform/abc_supplier_template_ps_nofam","/IntoPlatform/abc_supplier_template_cost_nofam","/IntoPlatform/abc_supplier_template_cost_fam"]

remotapath_parent_list = remotapath_parent_list_prod if rtEnvironment == "Prod" else remotapath_parent_list_demo

print(files)
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

sbm_Function_v2.saveTable('abc_supplier_template', 'parquet', 'overwrite', abc_supplier_template)
abc_supplier_template.write.format("parquet").option("header","true").mode("overwrite").save("/mnt/sbm_refined/abc_supplier_template")
# abc_supplier_template.write.format("parquet").option("header","true").mode("overwrite").save("/mnt/sppo-refined-dev/abc_supplier_template")

# COMMAND ----------

dbutils.notebook.exit(notebookReturnSuccess)
