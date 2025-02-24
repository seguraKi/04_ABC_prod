# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook info:
# MAGIC
# MAGIC -  **Business context explanation:**  The notebook generates ivy_prod_agreement_template sent to Jaggaer
# MAGIC - **Base tables**: 
# MAGIC   - guv_dim
# MAGIC   - material_dim
# MAGIC   - plant_dim
# MAGIC   - plant_material_dim
# MAGIC   - vendor_dim
# MAGIC   - planning_parameter_fact
# MAGIC   - active_agreement_fact
# MAGIC - **Developers involved:** Kenneth Aguilar, Mark Sanchez, Aaron Kubiesa
# MAGIC - **Frequency of running:**
# MAGIC   1. Daily
# MAGIC   
# MAGIC - Additional Comments: Moved into ABC folder from DefinitionOfLookup  because only this project was using this table. 
# MAGIC Relevant US https://dev.azure.com/SPPO-IT/SPPO%20and%20PCS/_workitems/edit/93931

# COMMAND ----------

# MAGIC %run "/Workspace/prod/01_INIT/SBMConfig"

# COMMAND ----------



# COMMAND ----------

spark.read.parquet("/mnt/mda-prod/site_attr_dim").createOrReplaceGlobalTempView("site_attr_dim")
spark.read.parquet("/mnt/mda-prod/material_attr_dim").createOrReplaceGlobalTempView("material_attr_dim")
spark.read.parquet("/mnt/mda-prod/planning_parameter_fact/").createOrReplaceGlobalTempView("planning_parameter_fact")
spark.read.parquet("/mnt/mda-prod/supplier_attr_dim").createOrReplaceGlobalTempView("supplier_attr_dim")
#spark.read.parquet("/mnt/mda-prod/supplier_attr_dim").createOrReplaceGlobalTempView("supplier_attr_dim")
spark.read.parquet("/mnt/mda-prod/site_attr_dim").createOrReplaceGlobalTempView("site_attr_dim")

spark.read.parquet("/mnt/refined/geo_dim").createOrReplaceGlobalTempView("geo_dim")
#_________________________________________________________________________

# spark.read.parquet("/mnt/refined/plant_dim").createOrReplaceGlobalTempView("plant_dim")
# spark.read.parquet("/mnt/refined/material_dim").createOrReplaceGlobalTempView("material_dim")
# spark.read.parquet("/mnt/mda-prod/planning_parameter_fact/").createOrReplaceGlobalTempView("planning_parameter_fact")
# spark.read.parquet("/mnt/refined/vendor_dim").createOrReplaceGlobalTempView("vendor_dim")
# spark.read.parquet("/mnt/refined/guv_dim").createOrReplaceGlobalTempView("guv_dim")
# spark.read.parquet("/mnt/refined/geo_dim").createOrReplaceGlobalTempView("geo_dim")



# COMMAND ----------

#v   iew_type = 'jaggaer_view'
#SBM_Tables_v2.createTable('vw_sbm_guv_dim', view_type)
#SBM_Tables_v2.createTable('vw_sbm_vendor_dim', view_type)
#SBM_Tables_v2.createTable('vw_sbm_plant_dim', view_type)
#SBM_Tables_v2.createTable('vw_sbm_material_dim', view_type)

# COMMAND ----------

#Using library script first. It's legacy behaviour
df_ivy_prod_agreement_template = SBM_Tables_v2.createTable('vw_ivy_prod_agreement_template', view_type)

# COMMAND ----------

sql_script = """
SELECT tbl.*, pop.ticket_custom_id FROM (
SELECT          cast(ppf.purchase_vendor_id as int) as purchase_vendor_id
                ,ppf.vendor_name
                ,cast(ppf.material_id as int) as material_id
                ,ppf.material_desc 
                ,concat_ws('|', sort_array(collect_set(ppf.business_unit))) as business_unit
                ,concat_ws('|', sort_array(collect_set(ppf.plant_region_override))) as region
                ,concat(ppf.plant_code, " : ", ppf.plant_name) as plant
                ,ifnull(b.base_unit_of_measure, 'n/a') as base_unit_of_measure
                ,ppf.plant_code
            FROM global_temp.planning_parameter_fact ppf 
            JOIN global_temp.vw_sbm_vendor_dim vv 
                    ON Cast(ppf.purchase_vendor_id AS INT) = vv.display_vendor_id 
            LEFT JOIN global_temp.vw_sbm_plant_dim as bb 
                    ON ppf.plant_code = bb.plant_code
            LEFT JOIN global_temp.vw_sbm_material_dim b
                ON cast(ppf.material_id as int) = cast(b.material_number as int)
            GROUP BY ppf.purchase_vendor_id, ppf.vendor_name, ppf.material_id, ppf.material_desc, b.base_unit_of_measure, concat(ppf.plant_code, " : ", ppf.plant_name), ppf.plant_code                
) tbl left join ivy_prod_agreement_prod  pop on tbl.purchase_vendor_id = pop.vendor_id and tbl.material_id=pop.material_id
"""
ivy_prod_agreement_template = spark.sql(sql_script)

# COMMAND ----------

files = sbm_Function_v2.splitDFtoCSV(ivy_prod_agreement_template, "/mnt/sbm_stage/sbm/","IVY_Prod_Agreement_Mult_VC_Template", 100000)
from datetime import datetime
from connectors import JaggaerConnector
from pathlib import Path
jaggaer_connector = JaggaerConnector("prod")
remotepath_parent = 'IntoPlatform/IVY_Prod_Agreement_Mult_VC_Template'
remotepath_parent2 = 'IntoPlatform/IVY_Prod_Agreement_Template'
list_of_localpaths = files
for f in list_of_localpaths:
  path_to_file = Path(f)
  file_name = path_to_file.name
  jaggaer_connector.put_file(str(path_to_file), remotepath_parent+"/"+file_name)
  jaggaer_connector.put_file(str(path_to_file), remotepath_parent2+"/"+file_name)
  os.rename(str(f), str(f)[:-4] + datetime.today().strftime('_%Y_%m_%d_%H_%M'))


# COMMAND ----------

#sbm_Function_v2.saveTable('ivy_prod_agreement_template', 'parquet', 'overwrite', ivy_prod_agreement_template)
ivy_prod_agreement_template.write.format("parquet").option("header","true").mode("overwrite").save("/mnt/sbm_refined/ivy_prod_agreement_template")
ivy_prod_agreement_template.write.format("parquet").option("header","true").mode("overwrite").save("/mnt/refined/ivy_prod_agreement_template")
