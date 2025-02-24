# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC ### Notebook info:
# MAGIC
# MAGIC - **Business context explanation:** Data that will be used by Rapid Response to execute the needed calculations to generate the different ABC scenarios
# MAGIC - **Base tables**: 
# MAGIC      1. active_agreement_fact
# MAGIC      2. plannng_parameter_fact
# MAGIC      3. sync_measures_fact
# MAGIC      4. abc_supplier_input_fact
# MAGIC      
# MAGIC - **Developers involved:**  Steph Cartin, Artur Sarata
# MAGIC
# MAGIC - **Aditional notes:**
# MAGIC   1. DFC = Days Forward Consumption/Coverage
# MAGIC   
# MAGIC - **Frequency of running:**
# MAGIC   1. Daily
# MAGIC

# COMMAND ----------

#%run
#"/Workspace/prod/Automation/Quality/DQConfig" 

# COMMAND ----------

# DBTITLE 0,Import
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, DecimalType
from pyspark.sql.window import Window
from pyspark.sql.functions import lower, regexp_replace, lpad, lit, coalesce,concat_ws,collect_list,when, col,sum, count, round, countDistinct
from pyspark.sql.functions import max as sparkMin
import pandas as pd
from pyspark.sql.functions import explode, array, col

# COMMAND ----------

# DBTITLE 1,Read Data
rtEnvironment="Prod"


agr = spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y'").alias('agr')
plan = spark.read.parquet("/mnt/mda-prod/planning_parameter_fact/").alias('plan')
sync = spark.read.parquet("/mnt/mda-prod/sync_measures_fact/").alias('syn')
abc = spark.read.parquet("/mnt/sbm_refined/abc_supplier_input_dim")
abc_tg_sum = spark.read.parquet("/mnt/sbm_refined/abc_trigger_summary_dim").filter("commercial_transparency_flag != 'yes'")
abc_fin = spark.read.parquet("/mnt/mda-prod/abc_financial_input_dim").alias('abc_fin')
conm = spark.read.parquet("/mnt/mda-prod/consumption_fcst_fact/").alias('conm')
iopt = spark.read.parquet("/mnt/mda-prod/iopt_pipo_material_dim/").withColumn('lower', lower(F.col('iopt_initiative_name'))).alias('iopt')
#ivy = spark.read.parquet("/mnt/mda-prod/ivy_material_master_dim/")
ivy = spark.read.parquet("/mnt/sbm_refined/material_storage_unit_dim")
plant = spark.read.parquet("/mnt/mda-prod/site_attr_dim/").alias('plant')
abc_fcst = spark.read.parquet("/mnt/sbm_refined/abc_fcst_override_fact")

df_price_material = spark.read.parquet('/mnt/mda-prod/price_material_fact')
df_material_dim = spark.read.parquet('/mnt/mda-prod/material_dim')

# COMMAND ----------

abc_fcst = abc_fcst.withColumn("plant_code", F.lpad(F.col('plant_code'), 4, '0')) \
                   .withColumn('vendor_id', F.lpad(F.col('vendor_id'), 10, '0')) \
                   .withColumn('material_id', F.lpad(F.col('material_id'), 18, '0')) \
                   .withColumn('ticket_id', F.lpad(F.col('ticket_id'), 7, '0')).alias('abc_fcst')


# COMMAND ----------

abc = abc.join(
    df_material_dim.select('material_number', 'material_desc'),
    abc.material_id == df_material_dim.material_number,  
    how='left'  
).drop(df_material_dim.material_number)  
abc = abc.withColumnRenamed('base_unit_of_measure', 'base_uom')

# COMMAND ----------

# DBTITLE 1,Cleans the data of the source tables
#Active Agreement Fact
agr = agr.select('agreement_naturalkey','source_system_code','purchase_vendor_id','plant_code','material_id','price_unit_factor','doc_curr_code','net_price_rate','price_purch_usd','spend_pool_lkp_code','spend_pool_group','spend_pool_medium_name','spend_pool_low_name','business_unit_lkp_code','sector_business_unit','business_unit')

#ABC Supplier Input Dim
abc = abc.select(
    lpad(F.col('vendor_id'), 10, '0').alias('vendor_id'),
    lpad(F.col('material_id'), 18, '0').alias('material_id'),
    F.col('material_desc'),
    F.col('base_uom'),
    F.col('mm_min_lot_size'),
    F.col('min_prod_qty'),
    F.col('abc_mat_fam_name'),
    F.col('abc_mat_fam_new'),
    F.col('abc_mat_fam_new_msm'),
    F.col('restriction_min_prod_qty'),
    F.col('restriction_max_prod_qty'),
    F.col('material_per_pallet_qty'),
    F.col('material_shelf_life_months'),
    F.col('ticket_id'),
    lpad(F.col('plant_code'), 4, '0').alias('plant_code'),
    F.col('pipo_indicator'),
    F.col('material_phase_out_phase_in'),
    F.col('initiative_name'),
    F.col('price_scale_fam_name'),
    F.col('supplier_uom'),
    F.col('supplier_uom_factor'),
    F.col('setup_cost_usd'),
    F.col('prod_rounding_value'),
    F.col('vendor_plant_change_freq'),
    F.col('supplier_input_repeats'),
    lit('').alias('supplier_storage_flag') 
)
abc.createOrReplaceTempView('abc')

#ABC Trigger Summary Dim
abc_tg_sum = abc_tg_sum.select('vendor_id','ticket_id', 'modified_date', 'ticket_owner', 'ticket_custom_id', 'ticket_title', 'pipo_horizon_days','forecast_override_flag','future_supplier_storage_flag').alias('tg')

# COMMAND ----------

# DBTITLE 1,Cleaning IOPT Data
iopt = iopt.withColumn('iopt_initiative_name', lower(F.col('iopt_initiative_name')))
iopt = iopt.withColumn('iopt_initiative_name',regexp_replace('iopt_initiative_name', '[^a-zA-Z0-9]', ' '))

# COMMAND ----------

# DBTITLE 0,Standardize plant-vendor-material
ivy = ivy.withColumn('vendor_id', lpad(F.col('vendor_id'),10,'0'))\
    .withColumn('material_id',lpad(F.col('material_id'),18,'0')).alias('ivy')

# COMMAND ----------

# DBTITLE 1,Get the family based in the last ticket_id
sql_script = '''with cte as (
                select vendor_id, material_id, plant_code, max(ticket_id) as ticket_id
                from abc
                group by  vendor_id, material_id, plant_code
              )
              select a.* from abc a
              join cte c on a.vendor_id = c.vendor_id and a.material_id = c.material_id and a.plant_code = c.plant_code and a.ticket_id = c.ticket_id
              '''
abc = spark.sql(sql_script)

# COMMAND ----------

# DBTITLE 1,Min IVY storage_unit_qty
ivy = ivy.select('vendor_id', 'material_id', 'storage_unit_quantity').groupBy('vendor_id', 'material_id').agg(sparkMin(F.col('storage_unit_quantity')).alias('storage_unit_quantity'))

# COMMAND ----------

# DBTITLE 1,STTM Columns
sttm_cols = ['agr.source_system_code','agr.purchase_vendor_id', 'agr.plant_code', 'agr.material_id', 'agr.price_unit_factor', 'agr.doc_curr_code', 'agr.net_price_rate', 'agr.price_purch_usd', 'plan.mm_minimum_lot_size','mm_minimum_lot_size_dfc','avg_batch_consumption_qty', 'abc.abc_mat_fam_name','abc.abc_mat_fam_new_msm', 'abc.restriction_min_prod_qty', 'abc.restriction_max_prod_qty', 'abc.supplier_storage_flag', 'material_per_pallet_qty',  'abc.min_prod_qty', 'min_prod_qty_dfc', 'agr.spend_pool_lkp_code', 'agr.spend_pool_group', 'agr.spend_pool_medium_name', 'agr.spend_pool_low_name', 'agr.business_unit_lkp_code', 'agr.sector_business_unit', 'agr.business_unit',coalesce('comma1.iopt_initiative_pi_material_id', 'comma2.iopt_initiative_pi_material_id').alias('iopt_initiative_pi_material_id'), coalesce('comma1.iopt_initiative_name','comma2.iopt_initiative_name').alias('iopt_initiative_name'), coalesce('pipo_indicator1', 'pipo_indicator2').alias('pipo_indicator'), coalesce('comma1.material_phase_out_phase_in','comma2.material_phase_out_phase_in').alias('material_phase_out_phase_in'),'tg.ticket_id', 'tg.modified_date', 'tg.ticket_owner', 'tg.ticket_custom_id', 'tg.ticket_title', 'tg.pipo_horizon_days', 'abc.setup_cost_usd', 'abc.prod_rounding_value', 'abc.material_shelf_life_months', 'abc.vendor_plant_change_freq', 'abc_fin.pg_nominal_wacc', 'abc_fin.scrap_extra_charges', 'timestamp']

# COMMAND ----------

# DBTITLE 1,Table join
plan_df = (agr.join(plan, F.col('agr.agreement_naturalkey') == F.col('plan.agreement_naturalkey'), 'inner')
          .selectExpr('agr.*','mm_minimum_lot_size')          
).alias('plan1')
plan_df.createOrReplaceTempView('plan_df')

# COMMAND ----------

# DBTITLE 1,mm_minimum_lot_size_dfc logic
# MAGIC %md
# MAGIC reapply DFC calculation from AAS model (exists in planparam fact)
# MAGIC
# MAGIC 1. mm_minimum_lot_size = DIVIDE([mm_minimum_lot_size], [qty_consumption_per_day], BLANK())
# MAGIC 2. qty_consumption_per_day = DIVIDE([qty_consumption_n13], 91, BLANK())
# MAGIC 3. qty_consumption_n13 = =SUMX( FILTER('rpm consumption_n13w_vw', 'rpm consumption_n13w_vw'[concat_plant_material]='core planning_parameter_fact'[concat_plant_material]), 'rpm consumption_n13w_vw'[sum_total_requirement_qty])
# MAGIC
# MAGIC Join consumption with planning parameter on plant_material combination to get the sum of sum_total_requirement_qty
# MAGIC

# COMMAND ----------

# DBTITLE 1,consumption
consumption_n13w = conm.where("requirement_date_for_the_component <= date_format(date_add(current_date(), 91),'yyyyMMdd')").alias('c')
consumption_n13w_filtered = consumption_n13w.join(plan, (F.col('plan.material_id') == F.col('c.material_id')) &
                                                        (F.col('plan.plant_code') == F.col('c.plant_code')), 'left')


qty_consumption_n13 = consumption_n13w_filtered.where("agreement_naturalkey is not null")\
                                                .groupBy('plan.agreement_naturalkey','c.material_id', 'c.material_type','c.plant_code')\
                                                .agg(F.sum('c.sum_total_Requirement_qty').alias('qty_consumption_n13'))

qty_consumption_per_day = qty_consumption_n13.withColumn('qty_consumption_per_day', F.col('qty_consumption_n13') / 91)

#Select only the necessary columns 
qty_consumption_per_day = qty_consumption_per_day.select("agreement_naturalkey", "qty_consumption_per_day").alias('qty_n13w')

# COMMAND ----------

# DBTITLE 1,Consumption N12
consumption_n12 = conm.where("requirement_date_for_the_component <= date_format(date_add(current_date(), 365),'yyyyMMdd')").alias('n12')
consumption_n12_filtered = (consumption_n12.join(plan, (F.col('plan.material_id') == F.col('n12.material_id')) &
                                                        (F.col('plan.plant_code') == F.col('n12.plant_code')), 'left')
                            .selectExpr(
                              "n12.material_id",
                              "n12.plant_code",
                              "n12.sum_total_Requirement_qty",
                              "n12.material_type",
                              "plan.agreement_naturalkey"
                            )
)

#NEW
consumption_n12_grouped = (consumption_n12_filtered
                           .groupBy( "material_id", "plant_code", "material_type", "agreement_naturalkey")
                           .agg(F.sum("sum_total_Requirement_qty").alias("total_requirement_qty"))
).alias('qty_n12m')

# COMMAND ----------

# DBTITLE 1,Aggregate calculations
ppf_final_df = (plan_df
            .join(qty_consumption_per_day, F.col('plan1.agreement_naturalkey') == F.col('qty_n13w.agreement_naturalkey'),'left').
            join(consumption_n12_grouped, F.col('plan1.agreement_naturalkey') == F.col('qty_n12m.agreement_naturalkey'),'left')
            .selectExpr('plan1.*', 'qty_n13w.qty_consumption_per_day', 'qty_n12m.total_requirement_qty')
)
ppf_final_df = ppf_final_df.withColumn('mm_minimum_lot_size_dfc', F.round(F.col('mm_minimum_lot_size') / F.col('qty_consumption_per_day'),2)).alias('ppf')

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Transform sync_measure_fact
#join sync_measure_fact
sync_df = (ppf_final_df.join(sync,  (F.col('ppf.plant_code') == F.col('syn.plant_code')) &
                                (F.col('ppf.material_id') == F.col('syn.material_id')), 'left')
           .selectExpr('ppf.*',
                       'syn.batch_volume_uom',
                       'syn.Usage_Uom',
                       'syn.weight',
                       'syn.cov_usage',
                       'syn.inventory',
                       'syn.sync'
                       )
)

sync_df = sync_df.withColumnRenamed('batch_volume_uom', 'avg_batch_consumption_qty').alias('sy')


# COMMAND ----------

# DBTITLE 1,Join ABC
abc_filtered = abc.select('vendor_id', 'material_id','material_desc','base_uom', 'abc_mat_fam_name', 'abc_mat_fam_new_msm', 'restriction_min_prod_qty', 'restriction_max_prod_qty','plant_code','supplier_storage_flag', 'material_per_pallet_qty', 'min_prod_qty','ticket_id', 'setup_cost_usd', 'prod_rounding_value', 'material_shelf_life_months', 'vendor_plant_change_freq','supplier_input_repeats').distinct().alias('abc')
abc_filtered = abc_filtered.withColumnRenamed('vendor_id','purchase_vendor_id').alias('abc')

abc_df = (
    sync_df.join(
        abc_filtered,
        (F.col('abc.material_id') == F.col('sy.material_id')) &
        (F.col('abc.plant_code') == F.col('sy.plant_code')) &
        (F.col('abc.purchase_vendor_id') == F.col('sy.purchase_vendor_id')),
        'left'
    )
    .join(
        abc_fcst,
        (F.col('abc.material_id') == F.col('abc_fcst.material_id')) &
        (F.col('abc.ticket_id') == F.col('abc_fcst.ticket_id')) &
        (F.col('abc.purchase_vendor_id') == F.col('abc_fcst.vendor_id')),
        'left'
    )
    .join(
        abc_tg_sum,
        (F.col('abc.ticket_id') == F.col('tg.ticket_id')) &
        (F.col('abc.purchase_vendor_id') == F.col('tg.vendor_id')),
        'left'
    )
    .selectExpr(
        'abc.*',
        'sy.source_system_code',
        'sy.price_unit_factor',
        'sy.doc_curr_code',
        'sy.net_price_rate',
        'sy.qty_consumption_per_day',
        'sy.price_purch_usd',
        'sy.spend_pool_lkp_code',
        'sy.spend_pool_group',
        'sy.spend_pool_medium_name',
        'sy.spend_pool_low_name',
        'sy.business_unit_lkp_code',
        'sy.sector_business_unit',
        'sy.business_unit',
        'sy.mm_minimum_lot_size',
        "CASE WHEN tg.forecast_override_flag == 'yes' THEN abc_fcst.qty_forecast_override ELSE sy.total_requirement_qty END AS qty_forecast_n12",
        "tg.future_supplier_storage_flag AS future_flag_supplier_inv_storage",
        #"CASE WHEN tg.forecast_override_flag = 'yes' THEN abc_fcst.qty_forecast_override ELSE FLOOR(RANDOM() * (10000 - 1000 + 1)) + 1000 END AS qty_forecast_n12",
        'sy.mm_minimum_lot_size_dfc',
        #'sy.avg_consumption_qty',
        'sy.Usage_Uom',
        'sy.avg_batch_consumption_qty',
        'sy.weight',
        'sy.cov_usage',
        'sy.inventory',
        'sy.sync'
    )
)

abc_df = abc_df.withColumn('min_prod_qty_dfc', F.round(F.col('min_prod_qty')/ F.col('qty_consumption_per_day'),2 ))\
        .withColumn('timestamp',F.current_timestamp()).alias('abc2')

# COMMAND ----------

# DBTITLE 1,Exclude Duplicated tickets
abc_df = abc_df.where("abc2.supplier_input_repeats is null or abc2.supplier_input_repeats = 1").alias('abc3')

# COMMAND ----------

# DBTITLE 1,Join abc_financial_input_dim
# abc_df.select('plant_code').display()
abc_plant = (abc_df.join(plant, F.col('abc3.plant_code') == F.col('plant.site_code'),'left')\
                  .join(abc_fin, F.col('plant.country_code') == F.col('abc_fin.country_code'), 'left')
            .selectExpr('abc3.*',
                        'abc_fin.pg_nominal_wacc',
                        'abc_fin.scrap_extra_charges'
                        )
).alias('abc4')


# COMMAND ----------

ivy_pallet_df = (abc_plant.join(ivy.alias('ivy'), (F.col('abc4.purchase_vendor_id') == F.col('ivy.vendor_id')) &
                            (F.col('abc4.material_id') == F.col('ivy.material_id')), 'left')
                 .selectExpr('abc4.*', 'ivy.storage_unit_quantity')
)

ivy_pallet_df = ivy_pallet_df.withColumn('material_per_pallet_qty', F.when(ivy_pallet_df.storage_unit_quantity.isNull(), ivy_pallet_df.material_per_pallet_qty).otherwise(ivy_pallet_df.storage_unit_quantity)).alias('ivy_pallet')

# COMMAND ----------

win_ivy = Window.partitionBy('ivy_pallet.purchase_vendor_id', 'ivy_pallet.plant_code', 'ivy_pallet.material_id').orderBy(F.col('ivy_pallet.ticket_id').desc())
ivy_pallet_df = ivy_pallet_df.withColumn('rank', F.dense_rank().over(win_ivy))
ivy_pallet_df = ivy_pallet_df.filter( F.col('rank') == 1).drop('rank')

# COMMAND ----------

ivy_pallet_df = ivy_pallet_df.join(abc_tg_sum.alias("tg"), F.col("ivy_pallet.ticket_id") == F.col("tg.ticket_id"), "left").selectExpr('ivy_pallet.*','tg.modified_date','tg.ticket_owner','tg.ticket_custom_id','tg.ticket_title','tg.pipo_horizon_days').alias('ivy_pallet2')

# COMMAND ----------

iopt_comma = iopt.groupBy('iopt_initiative_po_material_id', 'plant_code','iopt_initiative_pi_material_id')\
                    .agg(concat_ws(", ", collect_list('iopt_initiative_name')).alias('iopt_initiative_name')).alias('iopt')\
          .withColumn('material_phase_out_phase_in', concat_ws(" - ",F.regexp_replace('iopt_initiative_po_material_id', r'^[0]*', ''),F.regexp_replace('iopt_initiative_pi_material_id', r'^[0]*', '') ))

# COMMAND ----------

iopt_po_pi = (
    ivy_pallet_df.join(
        iopt_comma.alias('comma1'),
        (F.col('comma1.iopt_initiative_po_material_id') == F.col('ivy_pallet2.material_id')) & 
        (F.col('comma1.plant_code') == F.col('ivy_pallet2.plant_code')),
        "left"
    )
    .selectExpr(
        'ivy_pallet2.*',
        'comma1.iopt_initiative_pi_material_id',
        'comma1.iopt_initiative_name',
        'comma1.material_phase_out_phase_in',
        'comma1.iopt_initiative_po_material_id'
    )
    .withColumn(
        'pipo_indicator',
        F.coalesce(
            when(col('comma1.iopt_initiative_po_material_id').isNotNull(), 'PO'),
            when(col('comma1.iopt_initiative_pi_material_id').isNotNull(), 'PI')
        )
    )
)

column_selected = iopt_po_pi.alias('s')

# COMMAND ----------

column_selected = column_selected.groupBy( 's.source_system_code','s.purchase_vendor_id', 's.plant_code', 's.material_id','s.material_desc','s.base_uom','future_flag_supplier_inv_storage', 's.price_unit_factor', 's.doc_curr_code', 's.net_price_rate', 's.price_purch_usd', 's.mm_minimum_lot_size', 's.mm_minimum_lot_size_dfc', 's.avg_batch_consumption_qty','s.qty_forecast_n12' ,'s.abc_mat_fam_name', 's.abc_mat_fam_new_msm', 's.restriction_min_prod_qty', 's.restriction_max_prod_qty', 's.supplier_storage_flag', 's.material_per_pallet_qty', 's.min_prod_qty', 's.min_prod_qty_dfc', 's.spend_pool_lkp_code', 's.spend_pool_group', 's.spend_pool_medium_name', 's.spend_pool_low_name', 's.business_unit_lkp_code', 's.sector_business_unit', 's.business_unit', 's.pipo_indicator', 'ticket_id', 'modified_date', 'ticket_owner', 'ticket_custom_id', 'ticket_title','pipo_horizon_days', 'setup_cost_usd', 'prod_rounding_value', 'material_shelf_life_months', 'vendor_plant_change_freq', 'pg_nominal_wacc', 'scrap_extra_charges','s.timestamp')\
     .agg(concat_ws(", ", collect_list('s.iopt_initiative_name')).alias('iopt_initiative_name'),
          concat_ws(", ", collect_list('s.iopt_initiative_pi_material_id')).alias('iopt_initiative_pi_material_id'),
          concat_ws(", ", collect_list('s.material_phase_out_phase_in')).alias('material_phase_out_phase_in'))
           

# COMMAND ----------

final = column_selected\
      .withColumnRenamed('net_price_rate', 'price_purch_rate')\
      .withColumnRenamed('price_purch_usd', 'price_purch_rate_usd')

# COMMAND ----------

final = final.withColumn("avg_batch_consumption_qty", F.when(final.avg_batch_consumption_qty.isNull(),"").otherwise(final.avg_batch_consumption_qty))\
            .withColumn('abc_mat_fam_name', F.when(final.abc_mat_fam_name.isNull(),"").otherwise(final.abc_mat_fam_name))\
            .withColumn('abc_mat_fam_new_msm', F.when(final.abc_mat_fam_new_msm.isNull(),"").otherwise(final.abc_mat_fam_new_msm))\
            .withColumn('restriction_min_prod_qty', F.when(final.restriction_min_prod_qty.isNull(),"").otherwise(final.restriction_min_prod_qty))\
            .withColumn('supplier_storage_flag',  F.when(final.supplier_storage_flag.isNull(),"").otherwise(final.supplier_storage_flag))\
            .withColumn('material_per_pallet_qty',  F.when(final.material_per_pallet_qty.isNull(),"").otherwise(final.material_per_pallet_qty))\
            .withColumn('restriction_max_prod_qty', F.when(final.restriction_max_prod_qty.isNull(),"").otherwise(final.restriction_max_prod_qty))\
            .withColumn('min_prod_qty', F.when(final.min_prod_qty.isNull(),"").otherwise(final.min_prod_qty))\
            .withColumn('min_prod_qty_dfc', F.when(final.min_prod_qty_dfc.isNull(),"").otherwise(final.min_prod_qty_dfc))\
            .withColumn('iopt_initiative_pi_material_id', F.when(final.iopt_initiative_pi_material_id.isNull(),"").otherwise(final.iopt_initiative_pi_material_id))\
            .withColumn('iopt_initiative_name', F.when(final.iopt_initiative_name.isNull(),"").otherwise(final.iopt_initiative_name))\
            .withColumn('pipo_indicator', F.when(final.pipo_indicator.isNull(),"").otherwise(final.pipo_indicator))\
            .withColumn('material_phase_out_phase_in', F.when(final.material_phase_out_phase_in.isNull(),"").otherwise(final.material_phase_out_phase_in)).alias('f')

# COMMAND ----------

group_df = final.select('purchase_vendor_id', 'plant_code', 'material_id', 'price_unit_factor', 'doc_curr_code').distinct()
group_df.createOrReplaceTempView('group_df')

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Case1: Both lines have same price unit factor and doc_curr_code > normal regular av 
# MAGIC 2. Case2: Both lines have same curr_code but different Unit_factor > transform unit factor to 1 and then get the avg 
# MAGIC 3. Case3: Both lines have same price_unit_factor, but different doc_curr_code > drop the line

# COMMAND ----------

case_1_sql = """select purchase_vendor_id, plant_code, material_id, doc_curr_code, count(1)
from group_df
group by purchase_vendor_id, plant_code, material_id, doc_curr_code
having count(1) = 1"""

case_1_df = spark.sql(case_1_sql).alias('c1')

# COMMAND ----------

case_2_sql = """select purchase_vendor_id, plant_code, material_id, doc_curr_code, count(1)
from group_df
group by purchase_vendor_id, plant_code, material_id, doc_curr_code
having count(1) > 1"""

case_2_df = spark.sql(case_2_sql).alias('c2')

# COMMAND ----------

case_3_sql = """select purchase_vendor_id, plant_code, material_id, price_unit_factor, count(1)
from group_df
group by purchase_vendor_id, plant_code, material_id, price_unit_factor
having count(1) > 1"""

case_3_df = spark.sql(case_3_sql).alias('c3')

# COMMAND ----------

# DBTITLE 1,Converting price_unit_factor to 1 for all case 2 scenarios
conv_case_2 = final.join(case_2_df, (F.col('f.purchase_vendor_id') == F.col('c2.purchase_vendor_id'))
                      &(F.col('f.plant_code') == F.col('c2.plant_code'))
                      &(F.col('f.material_id') == F.col('c2.material_id')))\
                      .select("f.*")

# COMMAND ----------

conv_case_2 = conv_case_2.withColumn('price_unit_factor',  lit(1))

# COMMAND ----------

conv_case_1 = final.join(case_1_df, (F.col('f.purchase_vendor_id') == F.col('c1.purchase_vendor_id'))
                      &(F.col('f.plant_code') == F.col('c1.plant_code'))
                      &(F.col('f.material_id') == F.col('c1.material_id')))\
                      .select("f.*").alias('conv_1')

# COMMAND ----------

union_cases = conv_case_1.union(conv_case_2).alias('cov')

# COMMAND ----------

case_3_union = union_cases.join(case_3_df, (F.col('cov.purchase_vendor_id') == F.col('c3.purchase_vendor_id'))
                    &(F.col('cov.plant_code') == F.col('c3.plant_code'))
                    &(F.col('cov.material_id') == F.col('c3.material_id')),'left_anti')\
                    .select("cov.*")

# COMMAND ----------

agg_df = case_3_union\
            .groupBy('source_system_code','purchase_vendor_id', 'plant_code', 'material_id','material_desc' ,'base_uom','future_flag_supplier_inv_storage','price_unit_factor', 'doc_curr_code', 'abc_mat_fam_name', 'abc_mat_fam_new_msm','qty_forecast_n12', 'restriction_min_prod_qty', 'restriction_max_prod_qty', 'supplier_storage_flag', 'material_per_pallet_qty', 'min_prod_qty', 'min_prod_qty_dfc', 'spend_pool_lkp_code', 'spend_pool_group', 'spend_pool_medium_name', 'spend_pool_low_name', 'business_unit_lkp_code', 'sector_business_unit', 'business_unit', 'iopt_initiative_pi_material_id', 'iopt_initiative_name','pipo_indicator','material_phase_out_phase_in','ticket_id', 'modified_date', 'ticket_owner', 'ticket_custom_id', 'ticket_title','pipo_horizon_days', 'setup_cost_usd', 'prod_rounding_value', 'material_shelf_life_months', 'vendor_plant_change_freq', 'pg_nominal_wacc', 'scrap_extra_charges','timestamp')\
            .agg( F.avg('price_purch_rate').alias('price_purch_rate'),
                  F.avg('price_purch_rate_usd').alias('price_purch_rate_usd'),
                  F.avg('mm_minimum_lot_size').alias('mm_minimum_lot_size'),
                  F.avg('mm_minimum_lot_size_dfc').alias('mm_minimum_lot_size_dfc'),
                  F.avg('avg_batch_consumption_qty').alias('avg_batch_consumption_qty'))

# COMMAND ----------

agg_df = agg_df.withColumn("avg_batch_consumption_qty", F.when(agg_df.avg_batch_consumption_qty.isNull(),"").otherwise(agg_df.avg_batch_consumption_qty))\
                .withColumn("mm_minimum_lot_size_dfc", F.when(agg_df.mm_minimum_lot_size_dfc.isNull(),"").otherwise(agg_df.mm_minimum_lot_size_dfc))\
                .withColumn("mm_minimum_lot_size", F.when(agg_df.mm_minimum_lot_size.isNull(),"").otherwise(agg_df.mm_minimum_lot_size))\
                .withColumn("ticket_id", F.when(agg_df.ticket_id.isNull(), "").otherwise(agg_df.ticket_id))\
                .withColumn("modified_date", F.when(agg_df.modified_date.isNull(), "").otherwise(agg_df.modified_date))\
                .withColumn("ticket_owner", F.when(agg_df.ticket_owner.isNull(), "").otherwise(agg_df.ticket_owner))\
                .withColumn("ticket_custom_id", F.when(agg_df.ticket_custom_id.isNull(), "").otherwise(agg_df.ticket_custom_id))\
                .withColumn("setup_cost_usd", F.when(agg_df.setup_cost_usd.isNull(), "").otherwise(agg_df.setup_cost_usd))\
                .withColumn("pg_nominal_wacc", F.when(agg_df.pg_nominal_wacc.isNull(), "").otherwise(agg_df.pg_nominal_wacc))\
                .withColumn("scrap_extra_charges", F.when(agg_df.scrap_extra_charges.isNull(), "").otherwise(agg_df.scrap_extra_charges))\
                .withColumn("material_shelf_life_months", F.when(agg_df.material_shelf_life_months.isNull(), "").otherwise(agg_df.material_shelf_life_months))\
                .withColumn("vendor_plant_change_freq", F.when(agg_df.vendor_plant_change_freq.isNull(), "").otherwise(agg_df.vendor_plant_change_freq))\
                .withColumn("pipo_horizon_days", F.when(agg_df.pipo_horizon_days.isNull(), "").otherwise(agg_df.pipo_horizon_days))\
                .withColumn("prod_rounding_value", F.when(agg_df.prod_rounding_value.isNull(), "").otherwise(agg_df.prod_rounding_value))\
                .withColumn("ticket_title", F.when(agg_df.ticket_title.isNull(), "").otherwise(agg_df.ticket_title))

agg_df.createOrReplaceTempView('agg_df')

# COMMAND ----------

sql_script = """SELECT*,
                    CONCAT('{ "Values": ["',material_id, '"', ',',
                              '"',material_desc, '"', ',',
                              '"',base_uom, '"', ',',
                              '"',future_flag_supplier_inv_storage, '"', ',',
                              '"',plant_code, '"', ',',
                              '"',purchase_vendor_id, '"', ',',
                              '"',spend_pool_lkp_code, '"', ',',
                              '"',business_unit_lkp_code, '"', ',',
                              '"',price_unit_factor, '"', ',',
                              '"',doc_curr_code, '"', ',',
                              '"',price_purch_rate, '"', ',',
                              '"',price_purch_rate_usd, '"',',',
                              '"',mm_minimum_lot_size, '"', ',',
                              '"',mm_minimum_lot_size_dfc, '"', ',',
                              '"',avg_batch_consumption_qty, '"', ',',
                              '"',qty_forecast_n12, '"', ',',
                              '"',abc_mat_fam_new_msm, '"', ',',
                              '"',abc_mat_fam_name, '"', ',',
                              '"',restriction_min_prod_qty, '"', ',',
                              '"',restriction_max_prod_qty, '"', ',',
                              '"',supplier_storage_flag, '"', ',',
                              '"',min_prod_qty , '"', ',',
                              '"',min_prod_qty_dfc, '"', ',',
                              '"',spend_pool_group, '"', ',',
                              '"',spend_pool_medium_name, '"', ',',
                              '"',spend_pool_low_name, '"', ',',
                              '"',sector_business_unit, '"', ','
                              '"',business_unit, '"', ','
                              '"',material_per_pallet_qty, '"', ','
                              '"',iopt_initiative_pi_material_id, '"', ','
                              '"',iopt_initiative_name, '"', ','
                              '"',pipo_indicator, '"', ','
                              '"',material_phase_out_phase_in, '"', ','
                              '"',ticket_id, '"', ','
                              '"',modified_date, '"', ','
                              '"',ticket_owner, '"', ','
                              '"',ticket_custom_id, '"', ','
                              '"',ticket_title, '"', ','
                              '"',timestamp, '"', ','
                              '"',setup_cost_usd, '"', ','
                              '"',pg_nominal_wacc, '"', ','
                              '"',scrap_extra_charges, '"', ','
                              '"',material_shelf_life_months, '"', ','
                              '"',vendor_plant_change_freq, '"', ','
                              '"',pipo_horizon_days, '"', ','                             
                              '"',prod_rounding_value, '"','] }') as jsonObject

                  from agg_df """

df_to_save = spark.sql(sql_script)
df_to_save.createOrReplaceTempView("df")


# COMMAND ----------

agg_df = agg_df.withColumn(
    "spend_material_current",
     F.col("price_purch_rate_usd") / F.col("price_unit_factor") * F.col("qty_forecast_n12")
)

agg_sum_new_msm_df = agg_df.select("ticket_custom_id","abc_mat_fam_new_msm","qty_forecast_n12","spend_material_current")
agg_sum_current_df = agg_df.select("ticket_custom_id","abc_mat_fam_name","qty_forecast_n12","spend_material_current")

#Groups by ticket to calculate the total spend for each ticket
spend_per_ticket = (agg_sum_new_msm_df.filter(agg_sum_new_msm_df["spend_material_current"].isNotNull()) 
                              .groupBy("ticket_custom_id") 
                              .agg(sum("spend_material_current").alias("total_ticket_spend"))
                              )

fam_new_msm_spend_df = (agg_sum_new_msm_df
                   .groupBy("ticket_custom_id", "abc_mat_fam_new_msm")
                   .agg(sum("spend_material_current").alias("total_family_new_msm_spend"))
)

fam_current_spend_df = (agg_sum_current_df
                   .groupBy("ticket_custom_id", "abc_mat_fam_name")
                   .agg(sum("spend_material_current").alias("total_family_spend"))
)

#Join between the spend dataframes
joined_df =( fam_new_msm_spend_df.join(
    spend_per_ticket,
    on="ticket_custom_id",
    how="left"
)
)
# Calculates the percentages
percentage_df = joined_df.withColumn(
    "percentage",
    round((col("total_family_new_msm_spend") / col("total_ticket_spend")) * 100, 2)
    ).withColumn("annual_fcst_consumption_fam", col("total_family_new_msm_spend").cast("int"))

# COMMAND ----------

# Adds the ABC Mat Fam Name to the dataframe
agg_df = (agg_df
    .withColumn(
    "current_prod_fam_ind",
    when(col("material_id").cast("int") == col("abc_mat_fam_name").cast("int"), "No").otherwise("Yes")
    ).withColumn(
    "future_prod_fam_ind",
    when(col("material_id").cast("int") == col("abc_mat_fam_new_msm").cast("int"), "No").otherwise("Yes")
    )
)

agg_df = (agg_df.alias('a').join(
          fam_current_spend_df.alias('b'), on=["ticket_custom_id", "abc_mat_fam_name"], how="left")
          .join(
          fam_new_msm_spend_df.alias('c'), on=["ticket_custom_id", "abc_mat_fam_new_msm"], how="left")
          .selectExpr('a.*','b.total_family_spend', 'c.total_family_new_msm_spend')
)

# COMMAND ----------

window_spec = Window.partitionBy('ticket_id', 'abc_mat_fam_name')
agg_df = agg_df.withColumn(
    'annual_fcst_cons_current',  
    F.when(F.col('current_prod_fam_ind') == 'No', F.col('qty_forecast_n12'))  
    .otherwise(
        F.sum(F.col('qty_forecast_n12')).over(window_spec)  
    )
)

# COMMAND ----------

agg_df = agg_df.withColumn(
    "current_annual_runs",
    when(col("current_prod_fam_ind") == "No", 
         round(col("qty_forecast_n12") / col("min_prod_qty"),1))
    .otherwise(round(col("annual_fcst_cons_current") / col("min_prod_qty"),1))
)

# COMMAND ----------

# Define the window for the accumulated calculation
window_spec = Window.partitionBy("ticket_custom_id").orderBy(F.col("percentage").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculates 'accumulated'
percentage_df = percentage_df.withColumn("accumulated", F.sum("percentage").over(window_spec))

# Creates column 'ABC' based on the conditions
abc_classification_df = percentage_df.withColumn(
    "abc_segment",
    F.when(F.col("accumulated") <= 80, "A")
     .when((F.col("accumulated") > 80) & (F.col("accumulated") <= 95), "B")
     .otherwise("C")
)

# COMMAND ----------

agg_df = (
  agg_df.alias('a')
  .join(abc_classification_df.alias('b'), (F.col('a.ticket_custom_id') == F.col('b.ticket_custom_id')) &
                            (F.col('a.abc_mat_fam_new_msm') == F.col('b.abc_mat_fam_new_msm'))
                            , how="left")
  .selectExpr("a.*",
              "b.percentage as family_percentage",
              "b.accumulated as ticket_accumulated",
              "b.abc_segment"
              )
)

# COMMAND ----------

sum_a = agg_df.groupBy("ticket_custom_id").agg(F.sum(F.when(agg_df.abc_segment == "A", agg_df.qty_forecast_n12).otherwise(0)).alias("sum_a"))
sum_b = agg_df.groupBy("ticket_custom_id").agg(F.sum(F.when(agg_df.abc_segment == "B", agg_df.qty_forecast_n12).otherwise(0)).alias("sum_b"))
sum_c = agg_df.groupBy("ticket_custom_id").agg(F.sum(F.when(agg_df.abc_segment == "C", agg_df.qty_forecast_n12).otherwise(0)).alias("sum_c"))
agg_df = agg_df.join(sum_a, on="ticket_custom_id", how="left") \
       .join(sum_b, on="ticket_custom_id", how="left") \
       .join(sum_c, on="ticket_custom_id", how="left")
#Creates the new column
agg_df = agg_df.withColumn(
    "current_annual_runs_weighted",
    F.when(agg_df.abc_segment == "A", (agg_df.current_annual_runs * agg_df.qty_forecast_n12) / agg_df.sum_a)
     .when(agg_df.abc_segment == "B", (agg_df.current_annual_runs * agg_df.qty_forecast_n12) / agg_df.sum_b)
     .otherwise((agg_df.current_annual_runs * agg_df.qty_forecast_n12) / agg_df.sum_c)
)

#Round the column
agg_df = agg_df.withColumn(
    "current_annual_runs_weighted",
    F.round(F.col("current_annual_runs_weighted"), 1)
)

# COMMAND ----------

result = agg_df.groupBy('ticket_custom_id').agg(
    count('*').alias('total_rows'),                
    countDistinct('abc_mat_fam_new_msm').alias('distinct_fam') 
)

# COMMAND ----------

#kinaxis_load = "/mnt/sbm_refined/abc_kinaxis_annual_runs_fact" if rtEnvironment =="Prod" else "/mnt/sbm_refined/abc_kinaxis_annual_runs_fact_dev"
kinaxis_df = agg_df
abc_trigger_df = spark.read.parquet("/mnt/sbm_refined/abc_trigger_summary_dim")
#material_change_frequency_df = spark.read.parquet("/mnt/mda-prod/material_change_frequency_dim")

# COMMAND ----------

#Select the necessary columns
kinaxis_df2 = kinaxis_df.select("ticket_id","material_id",'purchase_vendor_id','plant_code',"price_purch_rate_usd","price_unit_factor","abc_mat_fam_name","abc_mat_fam_new_msm","annual_fcst_cons_current","abc_segment","future_flag_supplier_inv_storage")

# COMMAND ----------

# CALCULATES: FORECAST_FAMILY, PROCE_CURRENT_USD_PER_UNIT, SPEND_FAMILY, MATERIAL_COUNT
kinaxis_df3 = (kinaxis_df2
    .withColumn("forecast_family",  
    F.when(F.col("future_flag_supplier_inv_storage") == "No", F.col("annual_fcst_cons_current"))
     .otherwise(
         F.sum(F.col("annual_fcst_cons_current")).over(Window.partitionBy("ticket_id","abc_mat_fam_new_msm"))  
     )
    ).withColumn("price_current_usd_per_unit", 
     F.col("price_purch_rate_usd") / F.col("price_unit_factor")
    ).withColumn("spend_gcas", 
     F.col("annual_fcst_cons_current") * F.col("price_current_usd_per_unit") 
    ).withColumn(
    "spend_family",  
    F.when(F.col("future_flag_supplier_inv_storage") == "No", F.col("spend_gcas"))  
     .otherwise(
         F.sum(F.col("spend_gcas")).over(Window.partitionBy("ticket_id","abc_mat_fam_new_msm")) 
     )
    ).withColumn(
        "material_count",  
        F.count(F.col("material_id")).over(Window.partitionBy("ticket_id","abc_mat_fam_new_msm"))
    )
)


# COMMAND ----------

window_spec = Window.partitionBy("ticket_id")

kinaxis_df4 = (kinaxis_df3.withColumn(
    "spend_gcas_percentage",
    F.round((F.col("spend_gcas") / F.sum("spend_gcas").over(window_spec)) ,2 ))
    .withColumn(
    "spend_family_percentage",
    F.round((F.col("spend_family") / F.sum("spend_gcas").over(window_spec)) ,2 )
)
)

# COMMAND ----------

grouped_df = kinaxis_df4.groupBy("ticket_id", "abc_mat_fam_new_msm") \
               .agg(F.round(F.avg("spend_family_percentage"), 2).alias("spend_family_percentage"))

sorted_df = grouped_df.orderBy(F.desc("ticket_id"), F.desc("spend_family_percentage"))

window_spec = Window.partitionBy("ticket_id").orderBy(F.desc("spend_family_percentage")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

sorted_final = sorted_df.withColumn("accumulated_family_percentage", F.sum("spend_family_percentage").over(window_spec))

kinaxis_df4 = kinaxis_df4.drop('spend_family_percentage')
kinaxis_df5 = kinaxis_df4.join(sorted_final, on=["ticket_id", "abc_mat_fam_new_msm"], how="left")

# COMMAND ----------

abc_calc_df = kinaxis_df5

# COMMAND ----------

mpq_scenarios_df1 = kinaxis_df

# COMMAND ----------

mpq_scenarios_df2 = mpq_scenarios_df1.select("ticket_id","material_id","plant_code","future_flag_supplier_inv_storage","abc_mat_fam_new_msm","annual_fcst_cons_current","restriction_min_prod_qty","restriction_max_prod_qty","mm_minimum_lot_size","material_per_pallet_qty","prod_rounding_value","price_purch_rate_usd","price_unit_factor","setup_cost_usd","pg_nominal_wacc","scrap_extra_charges","material_shelf_life_months","pipo_horizon_days","doc_curr_code","vendor_plant_change_freq")

mpq_scenarios_df3 = mpq_scenarios_df2.withColumn("forecast_family",  
    F.when(F.col("future_flag_supplier_inv_storage") == "No", F.col("annual_fcst_cons_current"))
     .otherwise(
         F.sum(F.col("annual_fcst_cons_current")).over(Window.partitionBy("ticket_id","abc_mat_fam_new_msm"))  
     )
)

mpq_scenarios_df4 = mpq_scenarios_df3.join(abc_trigger_df.select("ticket_id", "current_supplier_storage_flag"), on="ticket_id", how="left")
mpq_scenarios_df4 = (mpq_scenarios_df4.withColumnRenamed("current_supplier_storage_flag", "flag_supplier_inv_storage")
                                        .withColumnRenamed("material_shelf_life_months", "shelf_life")
                     )


# COMMAND ----------

mpq_scenarios_df5 = (mpq_scenarios_df4
    .withColumn(
    "percent_fcst_gcas_share",  
    F.round(F.col("annual_fcst_cons_current") / F.col("forecast_family"), 2) 
    ).withColumn("price_current_usd_per_unit", 
     F.col("price_purch_rate_usd") / F.col("price_unit_factor"))

)

# COMMAND ----------

iterations = array(*[lit(i) for i in range(1, 51)])
mpq_scenarios_df5 = mpq_scenarios_df5.withColumn("iteration", explode(iterations))

# COMMAND ----------

mpq_scenarios_df5 = mpq_scenarios_df5.withColumn("restriction_min_prod_qty", col("restriction_min_prod_qty").cast("decimal(10, 2)")) \
                                       .withColumn("restriction_max_prod_qty", col("restriction_max_prod_qty").cast("decimal(10, 2)"))

# COMMAND ----------


mpq_scenarios_df5 = mpq_scenarios_df5.withColumn("restriction_min_prod_qty", F.col("restriction_min_prod_qty").cast("decimal(10, 2)")) \
                                       .withColumn("restriction_max_prod_qty", F.col("restriction_max_prod_qty").cast("decimal(10, 2)"))


window_spec = Window.partitionBy("ticket_id")

restriction_df = mpq_scenarios_df5.groupBy(
    "ticket_id", 
    "abc_mat_fam_new_msm"
).agg(
    F.min("restriction_min_prod_qty").alias("fam_restriction_min_prod_qty"),
    F.min("restriction_max_prod_qty").alias("fam_restriction_max_prod_qty")
)

mpq_scenarios_df5 = mpq_scenarios_df5.join(
    restriction_df,
    on=["ticket_id", "abc_mat_fam_new_msm"],  # Columnas por las cuales se realiza el join
    how="left"  # Usamos un left join para mantener todas las filas del DataFrame original
)

# COMMAND ----------

mpq_scenarios_df6 = mpq_scenarios_df5.withColumn(
    "mpq_iteration",
    F.when(
        F.col("future_flag_supplier_inv_storage") == "No",
        (F.col("fam_restriction_max_prod_qty") - F.col("fam_restriction_min_prod_qty")) / 50 * F.col("iteration")
    ).otherwise(
        (F.col("fam_restriction_max_prod_qty") - F.col("fam_restriction_min_prod_qty")) / 50 * F.col("iteration")
    ).cast("double")  
)

mpq_scenarios_df6 = (mpq_scenarios_df6.withColumn(
            "mpq_iteration",
            F.ceil(F.col("mpq_iteration") / F.col("prod_rounding_value")) * F.col("prod_rounding_value")
    ).withColumn(
        "concat_family_iteration",  
        F.concat(F.col("abc_mat_fam_new_msm"), F.lit("_"), F.col("iteration")) 
    )
)


mpq_scenarios_df6 = mpq_scenarios_df6

mpq_scenarios_df6 = mpq_scenarios_df6.withColumnRenamed("future_flag_supplier_inv_storage", "future_flag_supplier_inv_storage")

# COMMAND ----------

mpq_scenarios_df6 = mpq_scenarios_df6.withColumn(
    "mpq_sc_alloc_uom",  
    F.col("mpq_iteration") * F.col("percent_fcst_gcas_share") 
)

# COMMAND ----------

mpq_scenarios_df6 = mpq_scenarios_df6.withColumn(
    "moq_sc_uom",
    F.when(F.col("flag_supplier_inv_storage") == "No", F.col("mpq_sc_alloc_uom")) 
     .when(F.col("mm_minimum_lot_size").isNull() | (F.col("mm_minimum_lot_size") == ""), F.col("material_per_pallet_qty"))  
     .otherwise(F.col("mm_minimum_lot_size"))  
)

mpq_scenarios_df6 = mpq_scenarios_df6.withColumn(
    "mpq_sc_alloc_dfc",  
    (F.col("mpq_sc_alloc_uom") * 365) / F.col("annual_fcst_cons_current")  
)

mpq_scenarios_df6 = mpq_scenarios_df6.withColumn(
    "moq_sc_usd",  
    F.col("moq_sc_uom") * F.col("price_current_usd_per_unit")  
)

# COMMAND ----------

mpq_scenarios_df6 = mpq_scenarios_df6.withColumn(
    "ide_risk_sc_usd",
    F.when(
        (F.col("annual_fcst_cons_current") * F.greatest(
            (F.col("mpq_sc_alloc_dfc") - F.col("shelf_life")),
            (F.col("mpq_sc_alloc_dfc") - F.col("pipo_horizon_days")),
            (F.col("mpq_sc_alloc_dfc") - F.col("vendor_plant_change_freq"))
        ) / 365 * F.col("price_current_usd_per_unit")) < 0,
        0  
    ).otherwise(
        (F.col("annual_fcst_cons_current") * F.greatest(
            (F.col("mpq_sc_alloc_dfc") - F.col("shelf_life")),
            (F.col("mpq_sc_alloc_dfc") - F.col("pipo_horizon_days")),
            (F.col("mpq_sc_alloc_dfc") - F.col("vendor_plant_change_freq"))
        ) / 365 * F.col("price_current_usd_per_unit"))  
    )
)

mpq_scenarios_df6 = mpq_scenarios_df6.withColumn(
    "log_scrap_sc_usd", 
    F.col("ide_risk_sc_usd") * F.col("scrap_extra_charges") 
)

mpq_scenarios_df6 = mpq_scenarios_df6.withColumn(
    "total_wacc_pg_sc_usd",  
    (F.col("pg_nominal_wacc") * F.col("moq_sc_usd")) / 2  
)

# COMMAND ----------

mpq_scenarios_df7 = mpq_scenarios_df6.join(
    abc_calc_df.select("material_id", "material_count"),  
    mpq_scenarios_df6.material_id == abc_calc_df.material_id,
    how="left"  
)
mpq_scenarios_df7 = mpq_scenarios_df7.withColumn(
    "setup_cost",  
    F.when(F.col("future_flag_supplier_inv_storage") == "No", (F.col("annual_fcst_cons_current") * F.col("setup_cost_usd")) / F.col("mpq_sc_alloc_uom"))  
     .otherwise(
         (F.col("annual_fcst_cons_current") * F.col("setup_cost_usd") / F.col("mpq_sc_alloc_uom")) / F.col("material_count") 
     )
)

mpq_scenarios_df7 = mpq_scenarios_df7.withColumn(
    "total_supply_chain_cost_gcas",  
    F.coalesce(F.col("setup_cost"), F.lit(0)) + 
    F.coalesce(F.col("total_wacc_pg_sc_usd"), F.lit(0)) + 
    F.coalesce(F.col("log_scrap_sc_usd"), F.lit(0)) + 
    F.coalesce(F.col("ide_risk_sc_usd"), F.lit(0))
)


# COMMAND ----------


sum_if_df = mpq_scenarios_df7.groupBy("concat_family_iteration").agg(
    F.sum("total_supply_chain_cost_gcas").alias("total_supply_chain_cost_family_agg")  
)


mpq_scenarios_df7 = mpq_scenarios_df7.join(
    sum_if_df,
    on="concat_family_iteration",  
    how="left" 
)


mpq_scenarios_df7 = mpq_scenarios_df7.withColumn(
    "supply_chain_cost_fam_level",  
    F.when(F.col("future_flag_supplier_inv_storage") == "No", F.col("total_supply_chain_cost_gcas")) 
     .otherwise(F.col("total_supply_chain_cost_family_agg")) 
)

mpq_scenarios_df7 = mpq_scenarios_df7.drop("total_supply_chain_cost_family_agg")

# COMMAND ----------

min_supply_chain_cost_df = mpq_scenarios_df7.groupBy(
    "ticket_id", 
    "abc_mat_fam_new_msm"
).agg(
    F.min("supply_chain_cost_fam_level").alias("min_supply_chain_cost_accross_fam_iterations")
)
mpq_scenarios_df7 = mpq_scenarios_df7.join(
    min_supply_chain_cost_df,
    on=["ticket_id", "abc_mat_fam_new_msm"], 
    how="left"
)

mpq_scenarios_df7 = mpq_scenarios_df7.withColumn(
    "min_supply_chain_cost_ind",
    F.when(
        F.col("min_supply_chain_cost_accross_fam_iterations") == F.col("supply_chain_cost_fam_level"),
        True
    ).otherwise(False)
)

# COMMAND ----------

min_optimal_mpq_df = (mpq_scenarios_df7 
    .filter(mpq_scenarios_df7.supply_chain_cost_fam_level.isNotNull()) 
    .filter(mpq_scenarios_df7.min_supply_chain_cost_ind == True) 
    .groupBy("abc_mat_fam_new_msm", "ticket_id") 
    .agg(F.min("mpq_iteration").alias("min_sc_cost_fam_iterations")))

result_df = abc_calc_df.join(
    min_optimal_mpq_df,
    (abc_calc_df.abc_mat_fam_new_msm == min_optimal_mpq_df.abc_mat_fam_new_msm) & 
    (abc_calc_df.ticket_id == min_optimal_mpq_df.ticket_id),
    how='left'
)

result_df = result_df.select(
    abc_calc_df["*"],  
    round(min_optimal_mpq_df['min_sc_cost_fam_iterations'], 2).alias('optimal_mpq')  
)



# COMMAND ----------

result_df = result_df.withColumn(
    "opt_annual_runs",  
    when(col("future_flag_supplier_inv_storage") == "No", col("annual_fcst_cons_current") / col("optimal_mpq"))  
    .otherwise(col("forecast_family") / col("optimal_mpq"))  
)

# COMMAND ----------

window_a = Window.partitionBy("ticket_id")

sum_ticket_A = F.sum(F.when(result_df.abc_segment == "A", result_df.forecast_family).otherwise(0)).over(window_a)
sum_ticket_B = F.sum(F.when(result_df.abc_segment == "B", result_df.forecast_family).otherwise(0)).over(window_a)
sum_ticket_C = F.sum(F.when(result_df.abc_segment == "C", result_df.forecast_family).otherwise(0)).over(window_a)

result_df = result_df.withColumn("sum_ticket_A", sum_ticket_A)\
       .withColumn("sum_ticket_B", sum_ticket_B)\
       .withColumn("sum_ticket_C", sum_ticket_C)

result_df = result_df.withColumn("opt_annual_runs_weighted",
                  F.round(
                   F.when(result_df.abc_segment == "A", result_df.opt_annual_runs * result_df.forecast_family / result_df.sum_ticket_A)
                    .when(result_df.abc_segment == "B", result_df.opt_annual_runs * result_df.forecast_family / result_df.sum_ticket_B)
                    .otherwise(result_df.opt_annual_runs * result_df.forecast_family / result_df.sum_ticket_C),2)
            )

# COMMAND ----------

joined_df = kinaxis_df.join(
    result_df.select("ticket_id",'purchase_vendor_id',"material_id", "plant_code", "opt_annual_runs_weighted"),
    on=["ticket_id", "purchase_vendor_id", "material_id", "plant_code"],
    how="left"
)

joined_df = joined_df.withColumn(
    "opt_annual_runs_weighted",
    F.when(F.col("setup_cost_usd").isNull() | (F.col("setup_cost_usd") == ""), None)
    .otherwise(F.col("opt_annual_runs_weighted"))
)

joined_df = joined_df \
    .withColumnRenamed("opt_annual_runs_weighted", "annual_run_segment_weighted_setup_cost") \
    .withColumnRenamed("current_annual_runs_weighted", "annual_run_segment_weighted")

df_to_save_final = joined_df.select("source_system_code"
,"purchase_vendor_id"
,"plant_code"
,"material_id"
,"price_unit_factor"
,"doc_curr_code"
,"price_purch_rate"
,"price_purch_rate_usd"
,"mm_minimum_lot_size"
,'mm_minimum_lot_size_dfc'
,"avg_batch_consumption_qty"
,"abc_mat_fam_name"
,"abc_mat_fam_new_msm"
,"restriction_min_prod_qty"
,"restriction_max_prod_qty"
,"supplier_storage_flag"
,"material_per_pallet_qty"
,"min_prod_qty"
,"min_prod_qty_dfc"
,"spend_pool_lkp_code"
,"spend_pool_group"
,"spend_pool_medium_name"
,"spend_pool_low_name"
,"business_unit_lkp_code"
,"sector_business_unit"
,"business_unit"
,"iopt_initiative_pi_material_id"
,"pipo_indicator"
,"material_phase_out_phase_in"
,"iopt_initiative_name"
,"ticket_id"
,"modified_date"
,"ticket_owner"
,"ticket_custom_id"
,"ticket_title"
,"setup_cost_usd"
,"prod_rounding_value"
,"material_shelf_life_months"
,"vendor_plant_change_freq"
,"pipo_horizon_days"
,"pg_nominal_wacc"
,"scrap_extra_charges"
,"annual_run_segment_weighted_setup_cost"
,"annual_run_segment_weighted"
,"abc_segment"
,"future_flag_supplier_inv_storage"
,"qty_forecast_n12"
,"material_desc"
,"base_uom")

df_to_save_final = df_to_save_final.select(
    *[
        F.when(F.col(column).isNull() | (F.trim(F.col(column)) == ""), None).otherwise(F.col(column)).alias(column)
        for column in df_to_save_final.columns
    ]
)

# COMMAND ----------

display(df_to_save_final)

# COMMAND ----------

save_path = "/mnt/sbm_refined/abc_kinaxis_input_fact" if rtEnvironment =="Prod" else "/mnt/sbm_refined/abc_kinaxis_input_fact_dev"
df_to_save_final.write.mode("overwrite").partitionBy("source_system_code").parquet(save_path)

# COMMAND ----------

display(df_to_save_final)

# COMMAND ----------

print(save_path)

# COMMAND ----------


