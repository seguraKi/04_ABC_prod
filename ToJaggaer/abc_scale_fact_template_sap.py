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

# MAGIC %run "../../01_INIT/SBMConfig"

# COMMAND ----------

# MAGIC %run "../../01_INIT/SBM_Logger"

# COMMAND ----------

set_log_identifiers(solution_name="ABC", pipeline_id=5, process_id=6)
log_process_start()

# COMMAND ----------

path_to_databricks_created_libraries = "/Workspace//prod/01_INIT"
sys.path.append(path_to_databricks_created_libraries)
import sbm_Function_v2

# COMMAND ----------

from jaggaer_connectors import JaggaerConnector
from pathlib import Path
from pyspark.sql.functions import col, count
from pyspark.sql import Row
#GoAnyWhere
jaggaer_connector = JaggaerConnector(rtEnvironment.lower())

# COMMAND ----------

# spark.read.parquet('/mnt/mda-prod/price_scale_dim').createOrReplaceTempView('ps')
# spark.read.parquet('/mnt/mda-prod/price_validity_dim').createOrReplaceTempView('pv')
#spark.read.parquet('/mnt/refined/active_agreement_fact').createOrReplaceTempView('aaf')
spark.read.parquet('/mnt/mda-prod/price_scale_dim').createOrReplaceTempView('ps')
spark.read.parquet('/mnt/mda-prod/price_validity_dim').createOrReplaceTempView('pv')
spark.read.parquet('/mnt/mda-prod/active_agreement_fact').createOrReplaceTempView('aaf')

# COMMAND ----------

abc_scale_fact_template_sap = """

WITH temp_pv AS (
  SELECT  
    MAX(valid_end_date) AS valid_end_date,
    source_system_code,
    purchase_doc_num,
    purchase_doc_line_num
  FROM pv 
  GROUP BY 
    source_system_code,
    purchase_doc_num,
    purchase_doc_line_num
),

temp_pvF AS (
  SELECT 
    tp.source_system_code,
    tp.purchase_doc_num,
    tp.purchase_doc_line_num,
    pv.cond_rec_num,
    tp.valid_end_date
  FROM temp_pv tp 
  INNER JOIN pv 
    ON pv.valid_end_date = tp.valid_end_date 
    AND pv.source_system_code = tp.source_system_code
    AND pv.purchase_doc_num = tp.purchase_doc_num
    AND pv.purchase_doc_line_num = tp.purchase_doc_line_num
)

SELECT DISTINCT 
    CAST(aaf.purchase_vendor_id AS INT) AS vendor_id,
    CONCAT(aaf.plant_code, ': ', aaf.plant_name) AS plant_code,
    CAST(aaf.material_id AS INT) AS material_id,
    aaf.order_uom AS scale_uom,
    ps.scale_quantity AS mpq_scale_qty,
    aaf.doc_curr_code,
    CAST(ps.rate AS DECIMAL(10,2)) AS scale_price,
    aaf.price_unit_factor,
    ps.valid_start_date,
    aaf.plant_code AS only_plant_code,
    aaf.source_system_code,
    aaf.purchase_doc_num,
    aaf.purchase_doc_line_num
FROM aaf
JOIN (
    SELECT 
      ps.scale_quantity,
      ps.rate,
      ps.valid_start_date,
      ps.source_system_code,
      ps.purchase_doc_num,
      ps.purchase_doc_line_num,
      ps.deletion_ind
    FROM ps 
    INNER JOIN temp_pvF pv
      ON ps.source_system_code = pv.source_system_code
      AND ps.purchase_doc_num = pv.purchase_doc_num
      AND ps.purchase_doc_line_num = pv.purchase_doc_line_num
      AND CAST(ps.cond_rec_num AS INT) = CAST(pv.cond_rec_num AS INT)
      AND ps.valid_end_date = pv.valid_end_date
      WHERE ps.deletion_ind <> 'X'
) ps ON (
    aaf.source_system_code = ps.source_system_code 
    AND aaf.purchase_doc_num = ps.purchase_doc_num 
    AND aaf.purchase_doc_line_num = ps.purchase_doc_line_num 
)
WHERE aaf.source_system_code IN ('F6PS4H', 'ANP430', 'L6P430', 'A6P430', 'N6P420') 
  AND aaf.source_list_flag = 'Y'
"""
abc_scale_fact_template_sap = spark.sql(abc_scale_fact_template_sap)

# COMMAND ----------

# filter_df = (abc_scale_fact_template_sap.filter( 
#                                                 (col("vendor_id") == '15237163') & 
#                                                 (col("material_id") == '21168539') & 
#                                                 (col("only_plant_code") =='A164') & 
#                                                 (col('mpq_scale_qty') == 7000)     
#                                               )
# )
# display(filter_df)

# COMMAND ----------

dups_df = abc_scale_fact_template_sap.groupBy("vendor_id", "material_id", "plant_code", "mpq_scale_qty") \
                .agg(count("*").alias("count")) \
                .filter(col("count") > 1)

# COMMAND ----------

abc_scale_fact_template_sap = abc_scale_fact_template_sap.alias("a").join(
    dups_df.alias("d"),
    (abc_scale_fact_template_sap.vendor_id == dups_df.vendor_id) &
    (abc_scale_fact_template_sap.material_id == dups_df.material_id) &
    (abc_scale_fact_template_sap.plant_code == dups_df.plant_code) &
    (abc_scale_fact_template_sap.mpq_scale_qty == dups_df.mpq_scale_qty),
    "left_anti"
)

# COMMAND ----------

#GoAnyWhere
files = sbm_Function_v2.splitDFtoCSV(abc_scale_fact_template_sap, "/mnt/sbm_stage/sbm/", "abc_scale_fact_template_sap", 100000)
remotepath_parent = 'IntoPlatform/abc_scale_fact_template_sap' if rtEnvironment == "Prod" else 'pg_demo/IntoPlatform/abc_scale_fact_template_sap'
list_of_localpaths = files
for f in list_of_localpaths:
  path_to_file = Path(f)
  file_name = path_to_file.name
  jaggaer_connector.put_file(str(path_to_file), remotepath_parent+"/"+file_name)
  os.rename(str(f), str(f)[:-4] + datetime.today().strftime('_%Y_%m_%d_%H_%M'))


# COMMAND ----------

log_process_complete(notebookReturnSuccess)

# COMMAND ----------

dbutils.notebook.exit(notebookReturnSuccess)
