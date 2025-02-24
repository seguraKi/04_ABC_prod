# Databricks notebook source
# MAGIC %run "../../01_INIT/SBM_Logger"

# COMMAND ----------

set_log_identifiers(solution_name="ABC", pipeline_id=3, process_id=1)
log_process_start()

# COMMAND ----------

# MAGIC %md
# MAGIC #Notebook info:
# MAGIC
# MAGIC
# MAGIC ####Developers involved: 
# MAGIC kubiesa.a.1@pg.com
# MAGIC
# MAGIC ####Use Cases scenarios: 
# MAGIC When there are duplicates we send information to the ticket owners about them. 
# MAGIC
# MAGIC ####Input tables:
# MAGIC abc_supplier_input_dim
# MAGIC abc_trigger_summary_dim
# MAGIC planning_parameter_fact
# MAGIC ####Reference Tables:
# MAGIC ####Target tables:

# COMMAND ----------

import sys
path_to_databricks_created_libraries = "/Workspace//prod/05_IVY/10_Misc/modules"
sys.path.append(path_to_databricks_created_libraries)

# COMMAND ----------

from emailing_module  import EmailingService
import pyspark.sql.functions as F
import re

# COMMAND ----------

abc_supplier_input_dim_df = spark.read.parquet("/mnt/sbm_refined/abc_supplier_input_dim")
#abc_supplier_input_dim_df = spark.read.parquet("/mnt/refined/abc_supplier_input_dim")
#abc_supplier_input_dim_df = spark.read.parquet("/mnt/mda-prod/abc_supplier_input_dim")
abc_trigger_summary_dim_df = spark.read.parquet("/mnt/sbm_refined/abc_trigger_summary_dim")
#abc_trigger_summary_dim_df = spark.read.parquet("/mnt/refined/abc_trigger_summary_dim")
#abc_trigger_summary_dim_df = spark.read.parquet("/mnt/mda-prod/abc_trigger_summary_dim")
#planning_parameter_fact_df = spark.read.parquet('/mnt/refined/planning_parameter_fact') 
planning_parameter_fact_df = spark.read.parquet('/mnt/mda-prod/planning_parameter_fact') 
#material,vendor,plant

# COMMAND ----------

duplicate_input_df = abc_supplier_input_dim_df.groupby("material_id", "plant_code", "vendor_id", "ticket_id").agg(F.count("*").alias("supplier_input_repeats")).filter("supplier_input_repeats>1")

# COMMAND ----------

mailing_list_df = (
    (
        duplicate_input_df.alias("a")
        .join(abc_trigger_summary_dim_df.groupby("ticket_id","ticket_owner","ticket_custom_id").agg(F.count(F.lit(1))).alias("b"), "ticket_id", "leftouter")
        .select("material_id", "plant_code", "a.vendor_id", "ticket_id", "supplier_input_repeats", "ticket_owner","ticket_custom_id")
            .join(
                planning_parameter_fact_df.groupby("material_id","purchase_vendor_id","ticket_id","ticket_owner").agg(F.count(F.lit(1))).alias("c"),
                [
                    F.substring(
                    F.concat(F.lit("0000000000"), F.col("vendor_id")), -10, 10
                ) == F.col("c.purchase_vendor_id"),
                    F.concat(
                F.substring(F.concat(F.lit("000000000000000000"), F.col("a.material_id")),-18,18,
                )) == F.col("c.material_id")
                ],
                "leftouter",
            )
            .selectExpr(
                "a.material_id", "a.plant_code", "a.vendor_id", "a.ticket_id", "supplier_input_repeats"," CASE WHEN b.ticket_owner IS NULL THEN c.ticket_owner ELSE b.ticket_owner END as ticket_owner","b.ticket_custom_id"
            )
        )
    ).cache()



# COMMAND ----------

owners_rdd = mailing_list_df.filter("ticket_owner!='bonilla.mb.1@pg.com'").select("ticket_owner").distinct().collect()

# COMMAND ----------

for row in owners_rdd:
    print(row['ticket_owner'])

# COMMAND ----------

emailer = EmailingService("prod")
template = emailer.provide_template()
regexp = re.compile(".+@pg.com")
for row in owners_rdd:
    print("Owner: ", row["ticket_owner"])
    owner = row["ticket_owner"]
    df = mailing_list_df.filter(F.col("ticket_owner") == F.lit(owner)).select(
        "material_id", "plant_code", "vendor_id", "ticket_custom_id", "supplier_input_repeats"
    )

    pd_df = df.toPandas()
    pd_df.rename(
        columns={
            "material_id": "Material code",
            "plant_code": "Plant code",
            "vendor_id": "Vendor code",
            "ticket_id": "Customer ticket id",
            "supplier_input_repeats": "Duplicate count",
        },
        inplace=True,
    )
    pd_df_html = pd_df.to_html(index=False, max_rows=100)

    # Determine the receiver (if the email is valid it will be ticket owner, otherwise Azul) and the content.
    if owner == None: owner=""
    if type(re.search(regexp, owner)) is re.Match:
        if re.search(regexp, owner).group() == owner:
            content = f"""        <p><b>Dear User,</b></p>
            <p>Your input for the ticket MPQ Optimization: Analysis trigger has duplicated information (plant, vendor and material). Please review the data and publish the ticket with the right information.</p>
            <p style="text-decoration: underline">
            <strong>SBM ticket information:</strong>
            </p>
            {pd_df_html}    
            """
            receiver = owner
            print('Email send to owner.')
        else:
            print(f'The ticket_owner "{owner}" is not matching the regex pattern. THere are additional values besides correct pattern string.')
            content = f"""        <p><b>Dear User,</b></p>
            <p style="text-decoration: underline">
            <strong>Ticket Owner is not valid email address, the message defaults to OOQ owner. Ticket owner: {owner}</strong>
            </p>
            <p>Your input for the ticket MPQ Optimization: Analysis trigger has duplicated information (plant, vendor and material). Please review the data and publish the ticket with the right information.</p>
            <p style="text-decoration: underline">
            <strong>SBM ticket information:</strong>
            </p>
            {pd_df_html}    
            """
            receiver = "miranda.av@pg.com"
    else:
        print(f"ticket_owner {owner} is not matching the regex pattern")
        content = f"""        <p><b>Dear User,</b></p>
            <p style="text-decoration: underline">
            <strong>Ticket Owner is not valid email address, the message defaults to OOQ owner. Ticket owner: {owner}</strong>
            </p>
            <p>Your input for the ticket MPQ Optimization: Analysis trigger has duplicated information (plant, vendor and material). Please review the data and publish the ticket with the right information.</p>
            <p style="text-decoration: underline">
            <strong>SBM ticket information:</strong>
            </p>
            {pd_df_html}    
            """
        receiver = "miranda.av@pg.com"

    template_filled = template.replace("CONTENT_PLACEHOLDER", content)

    subject = "MPQ Optimization: Analysis Trigger - Duplicated ticket"
    emailer.wrap_email(receiver, subject, template_filled)
    emailer.send_email()

# COMMAND ----------

log_process_complete()
