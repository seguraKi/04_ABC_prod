# Databricks notebook source
#%run "../../01_INIT/SBMConfig"

# COMMAND ----------

  # import os
  # import shutil
  # import argparse
  # import uuid
  # import glob
  # import pandas as pd 
  # import pyspark.sql.functions as f
  # from pyspark.sql.types import *
  # from pyspark.sql import SparkSession
  # from pyspark.sql.functions import input_file_name

# COMMAND ----------

# def session(session_name):
#   """Creates or retrives a Spark Session based on the session name."""
#   return SparkSession.builder.appName(session_name) \
#                           .config("spark.eventLog.enabled", "true") \
#                           .getOrCreate() 

# COMMAND ----------

# def readxcelworksheet(file_name, table_name):
#   #Set up the session
#   session_name = uuid.uuid1()
#   spark = session(session_name)
  
#   # import pandas as pd
#   # df = pd.read_excel (r'/dbfs/mnt/raw/lookup/rccp_mat_family/rccp_mat_fam.xlsx', sheet_name='rccp_mat_fam_map')
#   # spark_df = spark.createDataFrame(df)
  
#   #Load Table into the dataframe to be returned
#   sbm_DF = pd.read_excel(file_name.strip(),table_name)
#   sbm_DF.abc_mat_fam_new_msm = sbm_DF.abc_mat_fam_new_msm.astype(str)
#   sbm_DF_converted = spark.createDataFrame(sbm_DF)
#   sbm_DF_converted.createOrReplaceTempView(table_name)
  
#   return sbm_DF_converted

# COMMAND ----------

# try:
#   now_start = datetime.now()
#   title = 'Extracts'
  
#   df_abc_supplier_input_dim = readxcelworksheet('/dbfs/mnt/refined/sbm_data_operation/ABC/input_template/abc_supplier_input_dim.xlsx', 'Sheet1')
  
#   display(df_abc_supplier_input_dim)
  
# except Exception as ex:
#   sbm_Function_v2.captureLog('ASDW', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

# try:
#   now_start = datetime.now()
#   title = 'Insert ASDW'
  
#   rtEnvironment = "Prod"
#   sbm_Function_v2.saveASDW(rtEnvironment, "abc_supplier_input_dim", df_abc_supplier_input_dim, "sbm")
  
# except Exception as ex:
#   sbm_Function_v2.captureLog('ASDW', title, '', '', '0', '0', now_start, ex)
