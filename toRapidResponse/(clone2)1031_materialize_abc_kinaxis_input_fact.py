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

# DBTITLE 1,Import
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, DecimalType
from pyspark.sql.window import Window
from pyspark.sql.functions import lower, regexp_replace, lpad, lit, coalesce, concat_ws, collect_list, when, col
from pyspark.sql.functions import max as sparkMin

# COMMAND ----------

# DBTITLE 1,Read Data
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

# COMMAND ----------

# DBTITLE 1,Cleaning IOPT Data
iopt = iopt.withColumn('iopt_initiative_name', lower(F.col('iopt_initiative_name')))
iopt = iopt.withColumn('iopt_initiative_name',regexp_replace('iopt_initiative_name', '[^a-zA-Z0-9]', ' '))

# COMMAND ----------

# DBTITLE 0,Standardize plant-vendor-material

abc = abc.withColumn('plant_code',lpad(F.col('plant_code'),4,'0'))\
    .withColumn('vendor_id', lpad(F.col('vendor_id'),10,'0'))\
    .withColumn('material_id',lpad(F.col('material_id'),18,'0'))\
    .withColumn('supplier_storage_flag', lit('')).alias('abc')
abc.createOrReplaceTempView('abc')

ivy = ivy.withColumn('vendor_id', lpad(F.col('vendor_id'),10,'0'))\
    .withColumn('material_id',lpad(F.col('material_id'),18,'0')).alias('ivy')

# COMMAND ----------

# DBTITLE 1,Get the family based in the last ticket_id
sql_script = '''with cte as (
                select vendor_id, material_id, plant_code, max(ticket_id) as ticket_id
                from abc
                group by  vendor_id, material_id, plant_code
              )
              select * from abc a
              join cte c on a.vendor_id = c.vendor_id and a.material_id = c.material_id and a.plant_code = c.plant_code and a.ticket_id = c.ticket_id
              '''
abc = spark.sql(sql_script).select('a.vendor_id', 'a.material_id', 'a.qty_forecast_n12', 'a.base_unit_of_measure', 'a.mm_min_lot_size', 'a.min_prod_qty', 'a.abc_mat_fam_name', 'a.abc_mat_fam_new', 'a.abc_mat_fam_new_msm', 'a.restriction_min_prod_qty', 'a.restriction_max_prod_qty', 'a.material_per_pallet_qty', 'a.material_shelf_life_months', 'a.ticket_id', 'a.plant_code', 'a.pipo_indicator', 'a.material_phase_out_phase_in', 'a.initiative_name', 'a.price_scale_fam_name', 'a.supplier_uom', 'a.supplier_uom_factor', 'a.supplier_storage_flag', 'a.setup_cost_usd', 'a.prod_rounding_value', 'a.material_shelf_life_months', 'a.vendor_plant_change_freq','a.supplier_input_repeats'
)


# COMMAND ----------

# DBTITLE 1,Min IVY storage_unit_qty
ivy = ivy.select('vendor_id', 'material_id', 'storage_unit_quantity').groupBy('vendor_id', 'material_id').agg(sparkMin(F.col('storage_unit_quantity')).alias('storage_unit_quantity'))

# COMMAND ----------

# DBTITLE 1,STTM Columns
sttm_cols = ['agr.source_system_code','agr.purchase_vendor_id', 'agr.plant_code', 'agr.material_id', 'agr.price_unit_factor', 'agr.doc_curr_code', 'agr.net_price_rate', 'agr.price_purch_usd', 'plan.mm_minimum_lot_size','mm_minimum_lot_size_dfc','avg_batch_consumption_qty', 'abc.abc_mat_fam_name','abc.abc_mat_fam_new_msm', 'abc.restriction_min_prod_qty', 'abc.restriction_max_prod_qty', 'abc.supplier_storage_flag', 'material_per_pallet_qty',  'abc.min_prod_qty', 'min_prod_qty_dfc', 'agr.spend_pool_lkp_code', 'agr.spend_pool_group', 'agr.spend_pool_medium_name', 'agr.spend_pool_low_name', 'agr.business_unit_lkp_code', 'agr.sector_business_unit', 'agr.business_unit',coalesce('comma1.iopt_initiative_pi_material_id', 'comma2.iopt_initiative_pi_material_id').alias('iopt_initiative_pi_material_id'), coalesce('comma1.iopt_initiative_name','comma2.iopt_initiative_name').alias('iopt_initiative_name'), coalesce('pipo_indicator1', 'pipo_indicator2').alias('pipo_indicator'), coalesce('comma1.material_phase_out_phase_in','comma2.material_phase_out_phase_in').alias('material_phase_out_phase_in'),'tg.ticket_id', 'tg.modified_date', 'tg.ticket_owner', 'tg.ticket_custom_id', 'tg.ticket_title', 'tg.pipo_horizon_days', 'abc.setup_cost_usd', 'abc.prod_rounding_value', 'abc.material_shelf_life_months', 'abc.vendor_plant_change_freq', 'abc_fin.pg_nominal_wacc', 'abc_fin.scrap_extra_charges', 'timestamp']

# COMMAND ----------

# DBTITLE 1,Table join
plan_df = agr.join(plan, F.col('agr.agreement_naturalkey') == F.col('plan.agreement_naturalkey'), 'inner')
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

# DBTITLE 1,consumption next 13w 
consumption_n13w = conm.where("requirement_date_for_the_component <= date_format(date_add(current_date(), 91),'yyyyMMdd')").alias('c')
consumption_n13w_filtered = consumption_n13w.join(plan, (F.col('plan.material_id') == F.col('c.material_id')) &
                                                        (F.col('plan.plant_code') == F.col('c.plant_code')), 'left')


qty_consumption_n13 = consumption_n13w_filtered.where("agreement_naturalkey is not null")\
                                                .groupBy('plan.agreement_naturalkey','c.material_id', 'c.material_type','c.plant_code')\
                                                .agg(F.sum('c.sum_total_Requirement_qty').alias('qty_consumption_n13'))

qty_consumption_per_day = qty_consumption_n13.withColumn('qty_consumption_per_day', F.col('qty_consumption_n13') / 91).alias('qty')


# COMMAND ----------

# DBTITLE 1,Aggregate calculations
final_df = plan_df.join(qty_consumption_per_day, F.col('agr.agreement_naturalkey') == F.col('qty.agreement_naturalkey'),'left')
final_df = final_df.withColumn('mm_minimum_lot_size_dfc', F.round(F.col('plan.mm_minimum_lot_size') / F.col('qty.qty_consumption_per_day'),2))

# COMMAND ----------

# DBTITLE 1,Transform sync_measure_fact
#join sync_measure_fact
sync_df = final_df.join(sync,  (F.col('agr.plant_code') == F.col('syn.plant_code')) &
                                (F.col('agr.material_id') == F.col('syn.material_id')), 'left')

sync_df = sync_df.withColumnRenamed('batch_volume_uom', 'avg_batch_consumption_qty')

# COMMAND ----------

# DBTITLE 1,Join ABC
abc_filtered = abc.alias('abc').select('abc.vendor_id', 'abc.material_id', 'abc.abc_mat_fam_name', 'abc.abc_mat_fam_new_msm', 'abc.restriction_min_prod_qty', 'abc.restriction_max_prod_qty','abc.plant_code','abc.supplier_storage_flag', 'abc.material_per_pallet_qty', 'abc.min_prod_qty','abc.ticket_id', 'abc.setup_cost_usd', 'abc.prod_rounding_value', 'abc.material_shelf_life_months', 'abc.vendor_plant_change_freq','abc.supplier_input_repeats').distinct()

abc_df = sync_df.join(abc_filtered, (F.col('abc.material_id') == F.col('agr.material_id')) &
                          (F.col('abc.plant_code') == F.col('agr.plant_code')) &
                          (F.col('abc.vendor_id') == F.col('agr.purchase_vendor_id')),'left')

abc_df = abc_df.withColumn('min_prod_qty_dfc', F.round(F.col('abc.min_prod_qty')/ F.col('qty.qty_consumption_per_day'),2 ))\
        .withColumn('timestamp',F.current_timestamp())

# COMMAND ----------

# DBTITLE 1,Exclude Duplicated tickets
abc_df = abc_df.where("abc.supplier_input_repeats is null or abc.supplier_input_repeats = 1")

# COMMAND ----------

# DBTITLE 1,Join abc_financial_input_dim
# abc_df.select('plant_code').display()
abc_plant = abc_df.join(plant, F.col('abc.plant_code') == F.col('plant.site_code'),'left')\
                  .join(abc_fin, F.col('plant.country_code') == F.col('abc_fin.country_code'), 'left')


# COMMAND ----------

ivy_pallet_df = abc_plant.join(ivy.alias('ivy'), (F.col('agr.purchase_vendor_id') == F.col('ivy.vendor_id')) &
                            (F.col('agr.material_id') == F.col('ivy.material_id')), 'left')

ivy_pallet_df = ivy_pallet_df.withColumn('material_per_pallet_qty', F.when(ivy_pallet_df.storage_unit_quantity.isNull(), ivy_pallet_df.material_per_pallet_qty).otherwise(ivy_pallet_df.storage_unit_quantity))

# COMMAND ----------

win_ivy = Window.partitionBy('agr.purchase_vendor_id', 'agr.plant_code', 'agr.material_id').orderBy(F.col('abc.ticket_id').desc())
ivy_pallet_df = ivy_pallet_df.withColumn('rank', F.dense_rank().over(win_ivy))
ivy_pallet_df = ivy_pallet_df.filter( F.col('rank') == 1).drop('rank')

# COMMAND ----------

ivy_pallet_df = ivy_pallet_df.join(abc_tg_sum.alias("tg"), F.col("abc.ticket_id") == F.col("tg.ticket_id"), "left")

# COMMAND ----------

iopt_comma = iopt.groupBy('iopt_initiative_po_material_id', 'plant_code','iopt_initiative_pi_material_id')\
                    .agg(concat_ws(", ", collect_list('iopt_initiative_name')).alias('iopt_initiative_name')).alias('iopt')\
          .withColumn('material_phase_out_phase_in', concat_ws(" - ",F.regexp_replace('iopt_initiative_po_material_id', r'^[0]*', ''),F.regexp_replace('iopt_initiative_pi_material_id', r'^[0]*', '') ))

# COMMAND ----------

#iopt_comma.display()

# COMMAND ----------

iopt_po = ivy_pallet_df.join(iopt_comma.alias('comma1'), (F.col('comma1.iopt_initiative_po_material_id') == F.col('agr.material_id')) & (F.col('comma1.plant_code') == F.col('agr.plant_code')), "left")\
    .withColumn('pipo_indicator1', when(col('comma1.iopt_initiative_po_material_id').isNotNull(), 'PO').otherwise(None))
  
iopt_pi = iopt_po.join(iopt_comma.alias('comma2'), (F.col('comma2.iopt_initiative_pi_material_id') == F.col('agr.material_id')) & (F.col('comma2.plant_code') == F.col('agr.plant_code')), "left")\
    .withColumn('pipo_indicator2', when(col('comma2.iopt_initiative_pi_material_id').isNotNull(), 'PI').otherwise(None))
  
column_selected = iopt_pi.select(*sttm_cols).alias('s')

# COMMAND ----------

#iopt_po.display()

# COMMAND ----------

column_selected = column_selected.groupBy('s.source_system_code', 's.purchase_vendor_id', 's.plant_code', 's.material_id', 's.price_unit_factor', 's.doc_curr_code', 's.net_price_rate', 's.price_purch_usd', 's.mm_minimum_lot_size', 's.mm_minimum_lot_size_dfc', 's.avg_batch_consumption_qty', 's.abc_mat_fam_name', 's.abc_mat_fam_new_msm', 's.restriction_min_prod_qty', 's.restriction_max_prod_qty', 's.supplier_storage_flag', 's.material_per_pallet_qty', 's.min_prod_qty', 's.min_prod_qty_dfc', 's.spend_pool_lkp_code', 's.spend_pool_group', 's.spend_pool_medium_name', 's.spend_pool_low_name', 's.business_unit_lkp_code', 's.sector_business_unit', 's.business_unit', 's.pipo_indicator', 'ticket_id', 'modified_date', 'ticket_owner', 'ticket_custom_id', 'ticket_title','pipo_horizon_days', 'setup_cost_usd', 'prod_rounding_value', 'material_shelf_life_months', 'vendor_plant_change_freq', 'pg_nominal_wacc', 'scrap_extra_charges','s.timestamp')\
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
            .groupBy('source_system_code','purchase_vendor_id', 'plant_code', 'material_id', 'price_unit_factor', 'doc_curr_code', 'abc_mat_fam_name', 'abc_mat_fam_new_msm', 'restriction_min_prod_qty', 'restriction_max_prod_qty', 'supplier_storage_flag', 'material_per_pallet_qty', 'min_prod_qty', 'min_prod_qty_dfc', 'spend_pool_lkp_code', 'spend_pool_group', 'spend_pool_medium_name', 'spend_pool_low_name', 'business_unit_lkp_code', 'sector_business_unit', 'business_unit', 'iopt_initiative_pi_material_id', 'iopt_initiative_name','pipo_indicator','material_phase_out_phase_in','ticket_id', 'modified_date', 'ticket_owner', 'ticket_custom_id', 'ticket_title','pipo_horizon_days', 'setup_cost_usd', 'prod_rounding_value', 'material_shelf_life_months', 'vendor_plant_change_freq', 'pg_nominal_wacc', 'scrap_extra_charges','timestamp')\
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
                .withColumn("ticket_title", F.when(agg_df.ticket_title.isNull(), "").otherwise(agg_df.ticket_title)).createOrReplaceTempView('agg_df')


# COMMAND ----------

sql_script = """SELECT*,
                    CONCAT('{ "Values": ["',material_id, '"', ',',
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

df_to_save.printSchema()

# COMMAND ----------

#df_to_save.display()

# COMMAND ----------

# %sql
# select  purchase_vendor_id, plant_code, material_id, count(*)
# from df
# group by all
# having count(*)>1


# COMMAND ----------

df_to_save.display()

# COMMAND ----------

# DQFunction.expect_table_records_to_be_unique(df_to_save, 'abc_kinaxis_input_fact', False, ['source_system_code', 'purchase_vendor_id', 'plant_code', 'material_id', 'spend_pool_lkp_code', 'business_unit_lkp_code', 'iopt_initiative_pi_material_id', 'doc_curr_code'], False)
# if DQFunction.result == 'Success':
#   DQFunction.saveTable('abc_kinaxis_input_fact', 'parquet', 'overwrite', df_to_save, 'source_system_code')


df_to_save.write.mode("overwrite").partitionBy("source_system_code").parquet("/mnt/sbm_refined/abc_kinaxis_input_fact")
#df_to_save.write.mode("overwrite").partitionBy("source_system_code").parquet("/mnt/sppo-refined-dev/abc_kinaxis_input_fact")

# COMMAND ----------

# input_tables = ["/mnt/sppo-refined-dev/active_agreement_fact/",
#                 "/mnt/sppo-refined-dev/planning_parameter_fact/",
#                 "/mnt/sppo-refined-dev/sync_measures_fact/",
#                 "/mnt/refined/abc_supplier_input_dim/",
#                 "/mnt/refined/abc_trigger_summary_dim/",
#                 "/mnt/sppo-refined-dev/consumption_fcst_fact/",
#                 "/mnt/sppo-refined-dev/iopt_pipo_material_dim/",
#                 "/mnt/sppo-refined-dev/ivy_material_master_dim/"
#                 ]

# output_tables = ["/mnt/sbm_refined/abc_kinaxis_input_fact/"]

# DQFunction.rpm_SaveMetadata(input_tables, 'input')
# DQFunction.rpm_SaveMetadata(output_tables, 'output')

# COMMAND ----------

# DBTITLE 1,The End
dbutils.notebook.exit("all okay")

# COMMAND ----------

# DQFunction.saveASDW(rtEnvironment, "abc_kinaxis_input_fact", df_to_save, "rpm")

# COMMAND ----------

  # data = path.agg(
  #   F.concat_ws(",",F.collect_list(
  #     F.when(
  #       F.col("jsonObject").isNotNull(),
  #       F.regexp_replace(F.col("jsonObject"), r'[^\x00-\x7F]','')
  #     ).otherwise(F.col("jsonObject"))
  #   )).alias("cleanedJsonObject")
  # )

# COMMAND ----------

# from pyspark.sql.functions import col, regexp_extract

# problematic_chars = testData.withColumn(
#   "nonACSII",
#   regexp_extract(col("cleanedJsonObject"), r'[^\x00-\x7F]',0)
# ).filter(col("nonACSII") != '')
# problematic_chars.show(truncate=False)
