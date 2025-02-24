# Databricks notebook source
# MAGIC %run "../01_INIT/SBMConfig"

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType
import random
import smtplib, ssl
import api_curl_utilities
from email.message import EmailMessage
from email.utils import make_msgid
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.functions import max, split
from pyspark.sql import SparkSession
from openpyxl import Workbook
from openpyxl.styles import PatternFill
from io import BytesIO

#Add the script to the sys path
import sys
sys.path.insert(1, '/dbfs/FileStore/sbm/code/')
import sbm_utils

# COMMAND ----------

df_ppf = spark.read.format("parquet") \
    .load("/mnt/mda-prod/planning_parameter_fact") \
    .filter("modified_date >= date_sub(current_date(), 30)")

delta_table_path = "/mnt/sbm_refined/plan_param_delta" 
df_delta = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

#Removes the is_processed_column just to make the comparison and get the new inserts
df_delta_processed = df_delta.drop("is_processed")
df_new_inserts = df_ppf.subtract(df_delta_processed)

# COMMAND ----------

df_new_inserts = df_new_inserts.withColumn("is_processed", lit(False))

# COMMAND ----------

df_new_inserts.write.format("delta").mode("append").save(delta_table_path)
