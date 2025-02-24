# Databricks notebook source
# MAGIC %run "../../01_INIT/SBM_Logger"

# COMMAND ----------

set_log_identifiers(solution_name="ABC", pipeline_id=2, process_id=2)
log_process_start()

# COMMAND ----------

# MAGIC %md
# MAGIC #Notebook info:
# MAGIC **Business context explanation:** 
# MAGIC Selected users get email notification about mismatch between supplier_input_dim and price  scale_fact tables. Use price scale summary table as intermediary to join former two. If there is no join with price scale_summary, then related records coming from supplier_input_dim are ignored.
# MAGIC
# MAGIC We are taking the price_scale_fact_vw instead of our table, because there is additional logic used on ASDW and we need that logic to complete the task. Instead of repeating the logic we are downloading the table where it's already applied.
# MAGIC
# MAGIC **Developers involved:** 
# MAGIC kubiesa.a.1@pg.com
# MAGIC
# MAGIC **Use Cases scenarios:**
# MAGIC
# MAGIC 1. Join  supplier_input_dim and price_scale_summary by ticket_id  on ticket_id==supplier_input_ticket_id. We ignore records that have no join here.
# MAGIC 2. We match our table with price scale fact on ticket_id==ticket_id. Now there we need to have a match between vendor_id, price_scale_fam_name, plant_level. Otherwise user get's notified.
# MAGIC
# MAGIC **Input tables:**
# MAGIC
# MAGIC **Reference Tables:**
# MAGIC
# MAGIC **Target tables:**
# MAGIC
# MAGIC None
# MAGIC
# MAGIC

# COMMAND ----------

import sys
path_to_databricks_created_libraries = "/Workspace//prod/05_IVY/10_Misc/modules"
sys.path.append(path_to_databricks_created_libraries)

# COMMAND ----------

from emailing_module  import EmailingService
import pyspark.sql.functions as F
import re
from pyspark.sql import DataFrame
from connectors import JDBCConnector

# COMMAND ----------

jdbc_connector = JDBCConnector("prod")

# COMMAND ----------

def transformation(
    abc_supplier_input_dim: DataFrame,
    abc_price_summary: DataFrame,
    abc_price_fact: DataFrame,
) -> DataFrame:
    """
    Logic that was defined in US https://dev.azure.com/SPPO-IT/SPPO%20and%20PCS/_workitems/edit/90904
    Modified because of this US https://dev.azure.com/SPPO-IT/SPPO%20and%20PCS/_workitems/edit/93368
    """
    df1 = (
        abc_supplier_input_dim.alias("a")
        .join(
            abc_price_summary.alias("b"),
            [F.col("a.ticket_id") == F.col("b.supplier_input_ticket_id")],
            "leftouter",
        )
        .filter("b.supplier_input_ticket_id is not null")
        .selectExpr("a.*","b.ticket_id as price_summary_ticket_id", "b.ticket_owner","b.ticket_custom_id")
    )

    mismatch_df = (
        df1.alias("a")
        .join(
            abc_price_fact.alias("b"),
            [
                F.col("a.price_summary_ticket_id") == F.col("b.ticket_id"),
                F.col("a.vendor_id").cast("int") == F.col("b.vendor_id").cast("int"),
                F.col("a.material_id").cast("int") == F.col("b.material_id").cast("int"),
                F.col("a.plant_code") == F.col("b.plant_code"),
            ],
            "leftanti",
        )
        .selectExpr(["a.ticket_id ", "a.vendor_id", "a.material_id", "a.plant_code", "a.price_summary_ticket_id", "a.ticket_owner as ticket_owner_from_price_scale_summary","a.ticket_custom_id"])
    )

    return mismatch_df


# COMMAND ----------

def create_mailing_list(
    source: DataFrame,
    abc_trigger_summary_dim: DataFrame,
    planning_parameter_fact: DataFrame,
) -> DataFrame:
    """
    We want to know whom we should send the notification. This is done by finding ticket_owner. This information is taken from scale_summary, but we still need the Published in SBM status from trigger.
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
        .selectExpr(
            "a.material_id",
            "a.plant_code",
            "a.vendor_id",
            "a.ticket_id",
            "a.price_summary_ticket_id",
            "a.ticket_owner_from_price_scale_summary",
            "a.ticket_custom_id",
            "b.ticket_owner",
            "b.ticket_status"
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
            "b.ticket_id",
            "a.price_summary_ticket_id",
            "CASE WHEN a.ticket_owner_from_price_scale_summary IS NULL THEN c.ticket_owner ELSE ticket_owner_from_price_scale_summary END as ticket_owner",
            "a.ticket_owner_from_price_scale_summary", #it is here to remind of the source for testing purposes
            "a.ticket_custom_id"
        )
    )
    return mailing_list_df


# COMMAND ----------

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
    abc_price_fact =spark.read.parquet("/mnt/sbm_refined/abc_scale_fact")
    abc_price_summary = spark.read.parquet("/mnt/sbm_refined/abc_scale_summary_dim").filter("DATEDIFF(DAY,CAST(modified_date as DATE), getdate())<7 AND ticket_status='Published in SBM'")

    abc_trigger_summary_dim_df = spark.read.parquet(
        #"/mnt/sbm_refined/abc_trigger_summary_dim"
    #)  # join with abc_supplier_input_dim by ticket_id
    #abc_trigger_summary_dim_df = spark.read.parquet(
    #    "/mnt/refined/abc_trigger_summary_dim"
    #)  # join with abc_supplier_input_dim by ticket_id
    #abc_trigger_summary_dim_df = spark.read.parquet(
        "/mnt/mda-prod/abc_trigger_summary_dim"
    )  # join with abc_supplier_input_dim by ticket_

    #planning_parameter_fact_df = spark.read.parquet(
    #    "/mnt/refined/planning_parameter_fact"
    #)  # joined with main table by material,vendor,plant
    planning_parameter_fact_df = spark.read.parquet(
        "/mnt/mda-prod/planning_parameter_fact"
    )  # joined with main table by material,vendor,p

    mismatch_df = transformation(
        abc_supplier_input_dim=abc_supplier_input_dim,
        abc_price_summary=abc_price_summary,
        abc_price_fact=abc_price_fact,
    )
    mailing_df = create_mailing_list(
        source=mismatch_df,
        abc_trigger_summary_dim=abc_trigger_summary_dim_df,
        planning_parameter_fact=planning_parameter_fact_df,
    )

    owners_rdd = mailing_df.filter("ticket_owner!='bonilla.mb.1@pg.com'").select("ticket_owner").distinct().collect()

    emailer = EmailingService("prod")
    template = emailer.provide_template()
    regexp = re.compile(".+@pg.com")
    subject = "MPQ Optimization: Missing pricing information (Price Scales)"

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
