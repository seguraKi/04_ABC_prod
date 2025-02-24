# Databricks notebook source
# MAGIC %run "../../01_INIT/SBMConfig"

# COMMAND ----------

#Re-definition of the SFTP variables to be used only in Dev environment (since in Production these variables are already have the right SFTP settings
# print(rtEnvironment)
# if (rtEnvironment == "Dev"):
#   JaggaerXferSFTPuser = dbutils.secrets.get(scope = "sbm-keyvault", key = "JaggaerXferSFTPuser-prod")
#   JaggaerXferSFTPpswd = dbutils.secrets.get(scope = "sbm-keyvault", key = "JaggaerXferSFTPpswd-PROD")

# COMMAND ----------

# import datetime
# import time
# import sbm_Function
# import pandas as pd
# from datetime import datetime

try:
  now_start = datetime.now()
  title = 'Copy CSV'
  
  if rtEnvironment == 'Dev':
    sFTPPath = '/pg_demo/OutofPlatform/ABC/'
  else:
    sFTPPath = '/png_live/OutofPlatform/ABC/'
  
  sbm_Function_v2.transferFilesSFTP([], ABC_ASDWPath, sFTPPath, JaggaerXferSFTPHost, JaggaerXferSFTPuser, JaggaerXferSFTPpswd, 'get', jaggaer_rsa_key)
  
  sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start)
except Exception as ex:
  sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

try:
  now_start = datetime.now()
  title = 'Create Tables'
  
  sbm_Function_v2.saveTableDynamic(ABC_ASDWPath)
  
  sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start)
except Exception as ex:
  sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

# DBTITLE 1,copyDFtoDW
try: 
  now_start = datetime.now()
  title = 'copyDFtoDW'
  
  def copyDFtoDW(dir, url, user, pw, spark):
    #Check for proper formatting
    dir = dir[:-1] if dir[-1:] =="/" else  dir 
    dbfs_dir = "/dbfs"+ dir
    #Get files
    files = os.listdir(dbfs_dir)
    tbls = []
    #Get the files
    for file in files:
      #print(file)
      file_name = file.replace(".csv", "tempFileName").replace(".", "_")
      file_name = file_name.replace("tempFileName", ".csv")
      table_name = "sbm."+file_name[:-4]
      df=(spark
        .read
        .format("csv")
        .option("header", "true")
        .option("multiLine", "true")
        .option("delimiter", "|")
        .option("escape", '"')
        .load(dir + "/"+ file)
      )
      cols = df.columns
      #Handle Reserved Characters
      chars_to_replace = [' ',',','[',']']
      alias_cols = "~".join(col for col in cols)
      for char in chars_to_replace:
        alias_cols = alias_cols.replace(char,"_")
      more_chars = ['(',')','.','-','\\r']
      for char in more_chars:
        alias_cols = alias_cols.replace(char,'')

      new_cols = alias_cols.split('~')
      df2 = df.toDF(*new_cols)
      if (file_name[:-4] == "abc_supplier_input_dim"):
        df3 = df2.withColumn("restriction_min_prod_qty",col("restriction_min_prod_qty").cast("decimal(10,2)"))\
          .withColumn("restriction_max_prod_qty",col("restriction_max_prod_qty").cast("decimal(10,2)"))\
          .withColumn("material_per_pallet_qty",col("material_per_pallet_qty").cast("decimal(10,2)"))\
          .withColumn("material_shelf_life_months",col("material_shelf_life_months").cast("decimal(10,2)"))\
          .withColumn("min_prod_qty",col("min_prod_qty").cast("decimal(10,2)"))\
          .withColumn("qty_forecast_n12",col("qty_forecast_n12").cast("decimal(10,2)"))\
          .withColumn("mm_min_lot_size",col("mm_min_lot_size").cast("decimal(10,2)"))
        
        (df3.write
          .format("com.databricks.spark.sqldw")
          .mode("overwrite")
          .option("url",url )
          .option("maxStrLength","4000")
          .option("dbtable",table_name)
          .option("restriction_min_prod_qty", "restriction_min_prod_qty decimal(10,2)")
          .option("restriction_max_prod_qty", "restriction_max_prod_qty decimal(10,2)")
          .option("material_per_pallet_qty", "material_per_pallet_qty decimal(10,2)")
          .option("material_shelf_life_months", "material_shelf_life_months decimal(10,2)")
          .option("min_prod_qty", "min_prod_qty decimal(10,2)")
          .option("qty_forecast_n12", "qty_forecast_n12 decimal(10,2)")
          .option("mm_min_lot_size", "mm_min_lot_size decimal(10,2)")
          .option("useAzureMSI", "true")
          .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
          .option("user",user)
          .option("password",pw)
          .save()
        )
      else:
        (df2.write
          .format("com.databricks.spark.sqldw")
          .mode("overwrite")
          .option("url",url )
          .option("maxStrLength","4000")
          .option("dbtable",table_name)
          .option("useAzureMSI", "true")
          .option("tempDir","abfss://temp@saproddatahub.dfs.core.windows.net/sbm")
          .option("user",user)
          .option("password",pw)
          .save()
        )
      print(file_name + " written")
      tbls.append(table_name)
    return tbls

  ###------------------------------------------------------------------###
  sbm_Function_v2.captureLog('ASDW', title, '', '', '1', '0', now_start)
except Exception as ex:
  sbm_Function_v2.captureLog('ASDW', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

try:
  now_start = datetime.now()
  title = 'Upload CSV'
  
  #Upload the content of the CSV files into ASDW DB
  
  tables = copyDFtoDW(ABC_ASDWPath, url, jdbcUsername, jdbcPassword, spark)
  print(tables)
  
  sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start)
except Exception as ex:
  sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

# DBTITLE 1,Notebook_Reg
try:
  title = 'Notebook_Reg'
  
  sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start, '1')
except Exception as ex:
  sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

dbutils.notebook.exit(notebookReturnSuccess)
