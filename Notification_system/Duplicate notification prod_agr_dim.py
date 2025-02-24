# Databricks notebook source
import sys
path_to_databricks_created_libraries = "/Workspace//prod/05_IVY/10_Misc/modules"
sys.path.append(path_to_databricks_created_libraries)

# COMMAND ----------

from emailing_module  import EmailingService
import pyspark.sql.functions as F
import re

# COMMAND ----------

prod_agr_dim_df = spark.read.parquet("/mnt/sbm_refined/ivy_prod_agr_dim")
prod_agr_dim_df = spark.read.parquet("/mnt/refined/ivy_prod_agr_dim")
#prod_agr_dim_df = spark.read.parquet("/mnt/mda-prod/ivy_prod_agr_dim")

# COMMAND ----------

mailing_list_df = prod_agr_dim_df.groupBy("vendor_id","material_id","ticket_owner",'ticket_custom_id').agg(F.count(F.lit(1)).alias("prod_agr_repeats")).filter("prod_agr_repeats>1")

# COMMAND ----------

owners_rdd = mailing_list_df.filter("ticket_owner!='bonilla.mb.1@pg.com'").select("ticket_owner").distinct().collect()

# COMMAND ----------

emailer = EmailingService("prod")
template = emailer.provide_template()
regexp = re.compile(".+@pg.com")
for row in owners_rdd:
    print("Owner: ", row["ticket_owner"])
    owner = row["ticket_owner"]
    df = mailing_list_df.filter(F.col("ticket_owner") == F.lit(owner)).select(
        "material_id", "vendor_id", "ticket_custom_id", "prod_agr_repeats"
    )

    pd_df = df.toPandas()
    pd_df.rename(
        columns={
            "material_id": "Material code",
            "vendor_id": "Vendor code",
            "ticket_id": "Customer ticket id",
            "prod_agr_repeats": "Duplicate count",
        },
        inplace=True,
    )
    pd_df_html = pd_df.to_html(index=False, max_rows=100)

    # Determine the receiver (if the email is valid it will be ticket owner, otherwise Azul) and the content.
    if owner == None: owner=""
    if type(re.search(regexp, owner)) is re.Match:
        if re.search(regexp, owner).group() == owner:
            content = f"""        <p><b>Dear User,</b></p>
            <p>We have found duplicated information (material and vendor) in your MPQ Optimization: Production Agreement tickets. Please review the data and publish just one ticket with the correct information for the same material and vendor combination.</p>
            <p style="text-decoration: underline">
            <strong>SBM ticket information:</strong>
            </p>
            {pd_df_html}    
            """
            receiver = owner
        else:
            content = f"""        <p><b>Dear User,</b></p>
            <p style="text-decoration: underline">
            <strong>Ticket Owner is not valid email adress, the message defaults to OOQ owner. Ticket owner: {owner}</strong>
            </p>
            <p>We have found duplicated information (material and vendor) in your MPQ Optimization: Production Agreement tickets. Please review the data and publish just one ticket with the correct information for the same material and vendor combination.</p>
            <p style="text-decoration: underline">
            <strong>SBM ticket information:</strong>
            </p>
            {pd_df_html}    
            """
            receiver = "miranda.av@pg.com"
    else:
        content = f"""        <p><b>Dear User,</b></p>
            <p style="text-decoration: underline">
            <strong>Ticket Owner is not valid email adress, the message defaults to OOQ owner. Ticket owner: {owner}</strong>
            </p>
            <p>We have found duplicated information (material and vendor) in your MPQ Optimization: Production Agreement tickets. Please review the data and publish just one ticket with the correct information for the same material and vendor combination.</p>
            <p style="text-decoration: underline">
            <strong>SBM ticket information:</strong>
            </p>
            {pd_df_html}    
            """
        receiver = "miranda.av@pg.com"

    template_filled = template.replace("CONTENT_PLACEHOLDER", content)

    subject = "MPQ Optimization: Production Agreements - Duplicated ticket"
    emailer.wrap_email(receiver, subject, template_filled)
    emailer.send_email()
