# Databricks notebook source


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, DecimalType
from pyspark.sql.window import Window
from pyspark.sql.functions import lower, regexp_replace, lpad, lit, coalesce,concat_ws,collect_list,when, col,sum, count, round, countDistinct, ceil,min, rand
from pyspark.sql.functions import max as sparkMin
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array, col


# COMMAND ----------

rtEnvironment = "Prod"
kinaxis_load = "/mnt/sbm_refined/abc_kinaxis_annual_runs_fact" if rtEnvironment =="Prod" else "/mnt/sbm_refined/abc_kinaxis_annual_runs_fact_dev"
kinaxis_df = spark.read.parquet(kinaxis_load)
abc_trigger_df = spark.read.parquet("/mnt/sbm_refined/abc_trigger_summary_dim")
#material_change_frequency_df = spark.read.parquet("/mnt/mda-prod/material_change_frequency_dim")

# COMMAND ----------

# #ASSIGNS RANDOM NUMBER FOR FORECAST IF EMPTY
# kinaxis_df = kinaxis_df.withColumn(
#     "annual_fcst_cons_current",
#     F.when(
#         F.col("annual_fcst_cons_current").isNull(),
#         (F.rand() * (1000000 - 400000) + 400000).cast("integer")
#     ).otherwise(F.col("annual_fcst_cons_current"))
# ).withColumn('abc_mat_fam_new_msm', col('material_id'))

# COMMAND ----------

#Select the necessary columns
kinaxis_df2 = kinaxis_df.select("ticket_id","material_id",'purchase_vendor_id','plant_code',"price_purch_rate_usd","price_unit_factor","abc_mat_fam_name","abc_mat_fam_new_msm","annual_fcst_cons_current","abc_segment","future_flag_supplier_inv_storage","timestamp")

# COMMAND ----------

# CALCULATES: FORECAST_FAMILY, PRICE_CURRENT_USD_PER_UNIT, SPEND_FAMILY, MATERIAL_COUNT
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

# DBTITLE 1,MPQ Calculation
iterations = array(*[lit(i) for i in range(1, 51)])
mpq_scenarios_df5 = mpq_scenarios_df5.withColumn("iteration", explode(iterations))

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
    on=["ticket_id", "abc_mat_fam_new_msm"],  
    how="left" 
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


# COMMAND ----------

df_to_save_step2 = joined_df.select(
    *[
        F.when(F.col(column).isNull() | (F.trim(F.col(column)) == ""), "").otherwise(F.col(column)).alias(column)
        for column in joined_df.columns
    ]
)

df_to_save_step2.createOrReplaceTempView('df_to_save_step2')

# COMMAND ----------

sql_script = """SELECT *,

                    CONCAT('{ "Values": ["', material_id, '"', ',',
                              '"', plant_code, '"', ',',
                              '"', purchase_vendor_id, '"', ',',
                              '"', spend_pool_lkp_code, '"', ',',
                              '"', business_unit_lkp_code, '"', ',',
                              '"', price_unit_factor, '"', ',',
                              '"', doc_curr_code, '"', ',',
                              '"', price_purch_rate, '"', ',',
                              '"', price_purch_rate_usd, '"', ',',
                              '"', mm_minimum_lot_size, '"', ',',
                              '"', mm_minimum_lot_size_dfc, '"', ',',
                              '"', avg_batch_consumption_qty, '"', ',',
                              '"', abc_mat_fam_new_msm, '"', ',',
                              '"', abc_mat_fam_name, '"', ',',
                              '"', restriction_min_prod_qty, '"', ',',
                              '"', restriction_max_prod_qty, '"', ',',
                              '"', supplier_storage_flag, '"', ',',
                              '"', min_prod_qty, '"', ',',
                              '"', min_prod_qty_dfc, '"', ',',
                              '"', spend_pool_group, '"', ',',
                              '"', spend_pool_medium_name, '"', ',',
                              '"', spend_pool_low_name, '"', ',',
                              '"', sector_business_unit, '"', ',',
                              '"', business_unit, '"', ',',
                              '"', material_per_pallet_qty, '"', ',',
                              '"', iopt_initiative_pi_material_id, '"', ',',
                              '"', iopt_initiative_name, '"', ',',
                              '"', pipo_indicator, '"', ',',
                              '"', material_phase_out_phase_in, '"', ',',
                              '"', ticket_id, '"', ',',
                              '"', modified_date, '"', ',',
                              '"', ticket_owner, '"', ',',
                              '"', ticket_custom_id, '"', ',',
                              '"', ticket_title, '"', ',',
                              '"', timestamp, '"', ',',
                              '"', setup_cost_usd, '"', ',',
                              '"', pg_nominal_wacc, '"', ',',
                              '"', scrap_extra_charges, '"', ',',
                              '"', material_shelf_life_months, '"', ',',
                              '"', vendor_plant_change_freq, '"', ',',
                              '"', pipo_horizon_days, '"', ',',
                              '"', prod_rounding_value, '"', ',',
                               '] }') as jsonObject
                   from df_to_save_step2 """

df_to_save_final = spark.sql(sql_script)

# COMMAND ----------

df_to_save_final = df_to_save_final.select("source_system_code"
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
,"base_uom"
,"timestamp"
,"jsonObject")

# COMMAND ----------

df_to_save_final_rpm = df_to_save_final.select("source_system_code"
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

# COMMAND ----------

print(df_to_save_final.columns)

# COMMAND ----------

save_path = "/mnt/sbm_refined/abc_kinaxis_input_fact" if rtEnvironment =="Prod" else "/mnt/sbm_refined/abc_kinaxis_input_fact_dev"
save_path_rpm = "/mnt/sbm_refined/abc_kinaxis_fact" if rtEnvironment =="Prod" else "/mnt/sbm_refined/abc_kinaxis_fact_dev"

df_to_save_final.write.mode("overwrite").partitionBy("source_system_code").parquet(save_path)
df_to_save_final_rpm.write.mode("overwrite").parquet(save_path_rpm)

# COMMAND ----------

dbutils.notebook.exit("all okay")

# COMMAND ----------


