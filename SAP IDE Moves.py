# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook info:
# MAGIC
# MAGIC -  **Business context explanation:**  
# MAGIC - **Base tables**: active_agreement_fact, price_material_fact, lead_vendor_fact, latest_exchange_rate, mseg
# MAGIC - **Developers involved:** Kenneth Aguilar, Aaron Kubiesa
# MAGIC - **Frequency of running:** 

# COMMAND ----------

# MAGIC %run "../01_INIT/SBMConfig"

# COMMAND ----------

# MAGIC %run "../01_INIT/SBM_Logger"

# COMMAND ----------

set_log_identifiers(solution_name="ABC", pipeline_id=5, process_id=2)
log_process_start()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, isnull


# COMMAND ----------

mseg_df = spark.read.parquet("/mnt/stage/mseg_matdoc", mergeSchema=True)

# COMMAND ----------

# active_agreement_fact_df=spark.read.parquet("/mnt/refined/active_agreement_fact")
# price_material_fact_df = spark.read.parquet("/mnt/refined/price_material_fact")
# lead_vendor_fact_df = spark.read.parquet("/mnt/refined/lead_vendor_fact") 
# exchange_rate_df = spark.read.parquet("/mnt/refined/exchange_rate_dim").createOrReplaceTempView("exchange_rate_dim")

active_agreement_fact_df=spark.read.parquet("/mnt/mda-prod/active_agreement_fact")
price_material_fact_df = spark.read.parquet("/mnt/mda-prod/price_material_fact")
lead_vendor_fact_df = spark.read.parquet("/mnt/mda-prod/lead_vendor_fact") 
exchange_rate_df = spark.read.parquet("/mnt/mda-prod/exchange_rate_dim").createOrReplaceTempView("exchange_rate_dim")
latest_exchange_rate_df = spark.sql("""select distinct exc.* from
	(select exchange_rate_type, currency_from, currency_to, max(exchange_rate_valid_from) as valid_date
	 from exchange_rate_dim
	 where exchange_rate_valid_from >= 20210701 and exchange_rate_type = 'M' and currency_to = 'USD'
	 group by exchange_rate_type, currency_from, currency_to) as latest_exc

	 inner join exchange_rate_dim exc
	 on exc.currency_from = latest_exc.currency_from
		and exc.currency_to = latest_exc.currency_to
		and exc.exchange_rate_valid_from = latest_exc.valid_date
		and exc.exchange_rate_type = latest_exc.exchange_rate_type""")

# COMMAND ----------

sumCurrency_df = (
    mseg_df
    .filter('bwart in ("551", "552", "555", "556")')
    .withColumn("date", F.to_date(F.col("doc_post_date"), "yyyyMMdd"))
    .filter(F.col("date") >= F.to_date(F.lit("2020-07-01")))  
    .groupBy("matnr", "werks", "waers", "bwart", "date")
    .agg(
        F.sum(F.col("dmbtr")).alias("local_amount_currency"),
        F.sum(F.col("menge")).alias("qty"),
    )
    .selectExpr(
        "matnr as material_id",
        "werks as plant_code",
        "waers as currency_code",
        "bwart as move_type_code",
        "local_amount_currency",
        "qty",
        "date",
    )
)


# COMMAND ----------

ivy_inv_movements_fact = (
    (
        sumCurrency_df.alias("a")
        .withColumn('fiscal_year_num',(F.date_format(F.add_months(F.to_date(F.col('a.date'), 'yyyyMMdd'), 6), 'yyyy')).cast('int'))
        .join(
            active_agreement_fact_df.alias("b"),
            [
                F.col("a.material_id") == F.col("b.material_id"),
                F.col("a.plant_code") == F.col("b.plant_code"),
            ],
            "leftouter",
        )
        .join(
            price_material_fact_df.select(
                "material_id",
                "plant_code",
                "price_std_per_unit_usd",
                "purch_doc_curr_code",
            )

            .alias("c"),
            [
                F.col("a.material_id") == F.col("c.material_id"),
                F.col("a.plant_code") == F.col("c.plant_code"),
                F.col("a.currency_code") == F.col("c.purch_doc_curr_code"),
            ],
            "leftouter",
        )
        .join(
            lead_vendor_fact_df.alias("d"), ["material_id", "plant_code"], "leftouter"
        )
        .filter("d.itm_rank=1")
        .join(latest_exchange_rate_df.alias("e"), [F.col("a.currency_code")==F.col("e.currency_from"),F.col("e.currency_to")==F.lit("USD")], "leftouter")
    )
    .selectExpr(
        "right(concat('000000000000000000', a.material_id), 18) as material_id",
        "right(concat('0000', a.plant_code), 4) as plant_code",
        "ifnull(b.business_unit, '') as business_unit",
        "cast(ifnull(a.local_amount_currency, 0) as decimal(22, 4)) as local_amount_currency",
        "a.currency_code",
        "a.move_type_code",
        "CASE WHEN a.currency_code ='USD' THEN a.local_amount_currency ELSE a.local_amount_currency*(e.exchange_rate/NULLIF(e.ratio_from,0)) END as USD_amount",
        "d.purchase_vendor_id",
        "date",
        "qty",
        "c.price_std_per_unit_usd",
        "b.business_unit_lkp_code",
        "fiscal_year_num"
    )

).distinct()

# COMMAND ----------

# sbm_Function_v2.saveTable('ivy_inv_movements_fact', 'parquet', 'overwrite', ivy_inv_movements_fact)
ivy_inv_movements_fact.write.format("parquet").option("header","true").mode("overwrite").save("/mnt/sbm_refined/ivy_inv_movements_fact")
# ivy_inv_movements_fact.write.format("parquet").option("header","true").mode("overwrite").save("/mnt/refined/ivy_inv_movements_fact")

# COMMAND ----------

# (ivy_inv_movements_fact.write
#   .format("com.databricks.spark.sqldw")
#   .mode("overwrite")
#   .option("url",url )
#   .option("maxStrLength","4000")
#   .option("dbtable","ivy_inv_movements_fact")
#   .option("local_amount_currency","local_amount_currency decimal(22,4)") 
#   .option("USD_amount","USD_amount decimal(22,4)") 
#   .option("useAzureMSI", "true")
#   .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
#   .option("user",jdbcUsername)
#   .option("password",jdbcPassword)
#   .save()
# )

# COMMAND ----------

log_process_complete(notebookReturnSuccess)
