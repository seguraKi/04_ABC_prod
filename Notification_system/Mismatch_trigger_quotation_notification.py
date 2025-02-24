# Databricks notebook source
# MAGIC %run "../../01_INIT/SBM_Logger"

# COMMAND ----------

set_log_identifiers(solution_name="ABC", pipeline_id=2, process_id=1)
log_process_start()

# COMMAND ----------

# MAGIC %md
# MAGIC #Notebook info:
# MAGIC **Business context explanation:** 
# MAGIC Selected users get email notification about mismatch between supplier_input_dim and quotation_fact tables. Use quotation summary table as intermediary to join former two. If there is no join with quotation_summary, then related records coming from supplier_input_dim are ignored. The assumption is that the input process is not completed in the latter case.
# MAGIC
# MAGIC **Developers involved:** 
# MAGIC kubiesa.a.1@pg.com
# MAGIC
# MAGIC **Use Cases scenarios:**
# MAGIC
# MAGIC If there will be no join between supplier_input_dim and quotation_summary it should not count as a mismatch. To summarise processing steps:
# MAGIC 1. Join  supplier_input_dim and quotation_summary by ticket_id  on ticket_id==supplier_input_ticket_id. We ignore records that have no join here.
# MAGIC 2. We match our table with quotation_fact on ticket_id==ticket_id. Now there we need to have a match between vendor_id, material_id, plant_level. Otherwise user get's notified.
# MAGIC
# MAGIC **Input tables:**
# MAGIC - source_df=abc_supplier_input_dim,
# MAGIC - abc_quotation_summary=abc_quotation_summary,
# MAGIC - abc_quotation_fact=abc_quotation_fact,
# MAGIC **Reference Tables:**
# MAGIC
# MAGIC **Target tables:**
# MAGIC
# MAGIC None
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC https://dev.azure.com/SPPO-IT/SPPO%20and%20PCS/_sprints/taskboard/ABC%20Team/SPPO%20and%20PCS/Price%20Management/Optimized%20Order%20Quantity/OOQ%20Sprint%2034?workitem=89260

# COMMAND ----------

import sys
path_to_databricks_created_libraries = "/Workspace//prod/05_IVY/10_Misc/modules"
sys.path.append(path_to_databricks_created_libraries)

# COMMAND ----------

from emailing_module  import EmailingService
import pyspark.sql.functions as F
import re
from pyspark.sql import DataFrame

# COMMAND ----------

def transformation(
    source_df: DataFrame,
    abc_quotation_summary: DataFrame,
    abc_quotation_fact: DataFrame,
) -> DataFrame:
    """
    Logic that was defined in US https://dev.azure.com/SPPO-IT/SPPO%20and%20PCS/_workitems/edit/89260?src=WorkItemMention&src-action=artifact_link.
    """
    df1 = (
        source_df.alias("a")
        .join(
            abc_quotation_summary.alias("b"),
            [F.col("a.ticket_id") == F.col("b.supplier_input_ticket_id")],
            "leftouter",
        )
        .filter("b.supplier_input_ticket_id is not null")
        .selectExpr("a.*","b.ticket_id as quotation_summary_ticket_id","b.ticket_custom_id")
    )

    mismatch_df = (
        df1.alias("a")
        .join(
            abc_quotation_fact.alias("b"),
            [
                F.col("a.quotation_summary_ticket_id") == F.col("b.ticket_id"),
                F.col("a.vendor_id") == F.col("b.vendor_id"),
                F.col("a.material_id") == F.col("b.material_id"),
                F.col("a.plant_code") == F.col("b.plant_code"),
            ],
            "leftanti",
        )
        .selectExpr(["a.ticket_id ", "a.vendor_id", "a.material_id", "a.plant_code", "a.quotation_summary_ticket_id","a.ticket_custom_id"])
    )

    return mismatch_df

def create_mailing_list(
    source: DataFrame,
    abc_trigger_summary_dim: DataFrame,
    planning_parameter_fact: DataFrame,
) -> DataFrame:
    """
    We want to know to whom we should send the notification. This is done by finding ticket_owner. This information should be either in abc_trigger_summary_dim or planning_parameter_fact table.
    """
    mailing_list_df = (
        source.alias("a")
        .join(
            abc_trigger_summary_dim.groupby(
                "ticket_id", "ticket_owner","ticket_status"
            )
            .agg(F.count(F.lit(1)))
            .alias("b"),
            "ticket_id",
            "leftouter",
        ).filter("b.ticket_status='Published in SBM'")
        .select(
            "a.material_id",
            "a.plant_code",
            "a.vendor_id",
            "a.ticket_id",
            "b.ticket_owner",
            "b.ticket_status",
            "a.ticket_custom_id",
            "a.quotation_summary_ticket_id"
        )
        .join(
            planning_parameter_fact.groupby(
                "material_id", "purchase_vendor_id", "ticket_id", "ticket_owner", "vendor_name"
            )
            .agg(F.count(F.lit(1)))
            .alias("c"),
            [
                F.substring(F.concat(F.lit("0000000000"), F.col("vendor_id")), -10, 10)
                == F.col("c.purchase_vendor_id"),
                F.concat(
                    F.substring(
                        F.concat(F.lit("000000000000000000"), F.col("a.material_id")),
                        -18,
                        18,
                    )
                )
                == F.col("c.material_id"),
            ],
            "leftouter",
        )
        .selectExpr(
            "a.material_id",
            "a.plant_code",
            "a.vendor_id",
            "c.vendor_name",
            "a.ticket_id",
            "CASE WHEN b.ticket_owner IS NULL THEN c.ticket_owner ELSE b.ticket_owner END as ticket_owner",
            "a.ticket_custom_id",
            "a.quotation_summary_ticket_id"
        )
    )
    return mailing_list_df

    
def correct_owner_content(pd_df_html:str, owner:str) -> str:
    """
    If owner will match our regex pattern she/he will receive this email template (filled)
    """
    content = f"""        <p><b>Dear User,</b></p>
            <p>Based on your scope definition on the ticket MPQ Optimization: Analysis trigger, there is missing pricing input for some materials. Please review the data and publish the ticket with all the information needed. </p>
            <p style="text-decoration: underline">
            <strong>SBM ticket information:</strong>
            </p>
            {pd_df_html}    
            """
    return content
def not_correct_owner_content(pd_df_html:str,owner:str) -> str:
    """
    If owner will NOT match our regex pattern we will send email to project owner. In this case Azul(miranda.av@pg.com)
    """
    content =  f"""        <p><b>Dear User,</b></p>
            <p style="text-decoration: underline">
            <strong>Ticket Owner is not valid email address, the message defaults to OOQ owner. Ticket owner: {owner}</strong>
            </p>
            <p>Based on your scope definition on the ticket MPQ Optimization: Analysis trigger, there is missing pricing input for some materials. Please review the data and publish the ticket with all the information needed. </p>
            <p style="text-decoration: underline">
            <strong>SBM ticket information:</strong>
            \n
            </p>
            {pd_df_html}    
            """
    return content

def prepare_df_html(owner:str, source_df: DataFrame) -> str:
    """
    Usually there is some DataFrame send in the email. It needs to be converted to HTML code. Owner is necessary to apply logic for selecting only the right rows from mailing_df.
    """

    df = source_df.filter(F.col("ticket_owner") == F.lit(owner)).selectExpr(
        "CAST(material_id as INT)as material_id", "plant_code", "CAST(vendor_id AS INT) as vendor_id","vendor_name" ,"ticket_custom_id"
    )
    pd_df = df.toPandas()
    pd_df.rename(
        columns={
            "material_id": "Material code",
            "plant_code": "Plant code",
            "vendor_id": "Vendor code",
            "vendor_name" : "Vendor name",
            "ticket_custom_id": "Customer ticket id"
        },
        inplace=True,
    )
    pd_df_html = pd_df.to_html(index=False, max_rows=100)   
    return pd_df_html

# COMMAND ----------

def main():
    abc_supplier_input_dim = spark.read.parquet("/mnt/sbm_refined/abc_supplier_input_dim")
    abc_quotation_fact = spark.read.parquet("/mnt/sbm_refined/abc_quotation_fact")
    abc_quotation_summary = spark.read.parquet("/mnt/sbm_refined/abc_quotation_summary_dim").filter("DATEDIFF(DAY,CAST(modified_date as DATE), getdate())<7 AND ticket_status='Published in SBM'")
    #abc_quotation_summary = spark.read.parquet("/mnt/refined/abc_quotation_summary_dim").filter("DATEDIFF(DAY,CAST(modified_date as DATE), getdate())<7 AND ticket_status='Published in SBM'")
    #abc_trigger_summary_dim_df = spark.read.parquet(
        #"/mnt/sbm_refined/abc_trigger_summary_dim"
    #)  # join with abc_supplier_input_dim by ticket_
    #abc_trigger_summary_dim_df = spark.read.parquet(
    #    "/mnt/refined/abc_trigger_summary_dim"
    #)  # join with abc_supplier_input_dim by ticket_id
    abc_trigger_summary_dim_df = spark.read.parquet(
        "/mnt/mda-prod/abc_trigger_summary_dim"
    )  # join with abc_supplier_input_dim by ticket_id
    #planning_parameter_fact_df = spark.read.parquet(
    #    "/mnt/refined/planning_parameter_fact"
    #)  # joined with main table by material,vendor,plant
    planning_parameter_fact_df = spark.read.parquet(
        "/mnt/mda-prod/planning_parameter_fact"
    )  # joined with main table by material,vendor,plant

    mismatch_df = transformation(
        source_df=abc_supplier_input_dim,
        abc_quotation_summary=abc_quotation_summary,
        abc_quotation_fact=abc_quotation_fact,
    )
    mailing_df = create_mailing_list(
        source=mismatch_df,
        abc_trigger_summary_dim=abc_trigger_summary_dim_df,
        planning_parameter_fact=planning_parameter_fact_df,
    )

    owners_rdd = mailing_df.filter("ticket_owner!='bonilla.mb.1@pg.com'").select("ticket_owner").distinct().collect()
    print(owners_rdd)

    emailer = EmailingService("prod")
    template = emailer.provide_template()
    regexp = re.compile(".+@pg.com")
    subject = "MPQ Optimization: Missing pricing information (ABC Quotation)"

    for row in owners_rdd:
        owner = row["ticket_owner"]
        print("Owner: ", owner)
        pd_df_html = prepare_df_html(owner, mailing_df)
        # Determine the receiver and the content. If the email is valid it will be ticket owner, otherwise Azul.
        if owner == None:
            owner = ""
        if type(re.search(regexp, owner)) is re.Match:
            if re.search(regexp, owner).group() == owner:
                content = correct_owner_content(pd_df_html, owner)
                receiver = owner
            else:
                print(
                    f'The ticket_owner "{owner}" is not matching the correct email pattern'
                )
                content = not_correct_owner_content(pd_df_html, owner)
                receiver = "miranda.av@pg.com"
        else:
            print(f"ticket_owner {owner} is not proper type to be analyzed with regex")
            content = not_correct_owner_content(pd_df_html, owner)
            receiver = "miranda.av@pg.com"
       
        template_filled = template.replace("CONTENT_PLACEHOLDER", content)
        emailer.wrap_email(receiver, subject, template_filled)
        emailer.send_email()

# COMMAND ----------

main()

# COMMAND ----------

log_process_complete()
