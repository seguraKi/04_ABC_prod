# Databricks notebook source
# MAGIC %run "../../01_INIT/SBMConfig"

# COMMAND ----------

spark.read.parquet("/mnt/refined/planning_parameter_fact").createOrReplaceGlobalTempView(TablePrefix+"planning_parameter_fact")
spark.read.parquet("/mnt/refined/active_agreement_fact").where("source_system_code in ('F6P430', 'L6P430', 'A6P430', 'ANP430', 'N6P420') and material_type in ('ROH', 'HALB') ").createOrReplaceGlobalTempView(TablePrefix+"active_agreement_fact")
spark.read.parquet("/mnt/sppo-refined-dev/material_regional_dim").createOrReplaceGlobalTempView(TablePrefix+"mara_sl")
spark.read.parquet("/mnt/sppo-refined-dev/consumption_fcst_fact").createOrReplaceGlobalTempView(TablePrefix+"consumption_fcst_fact")
spark.read.parquet("/mnt/refined/material_dim").createOrReplaceGlobalTempView(TablePrefix+"material_dim")


view_type = 'jaggaer_view'
SBM_Tables_v2.createTable('vw_sbm_material_dim', view_type)

# COMMAND ----------

print(TablePrefix)

# COMMAND ----------

try:
  now_start = datetime.now()
  title = 'Supplier_Input'
  
  sql = """

      with min_lot as (

      select distinct
         ppf.purchase_vendor_id as vendor_id
        , ppf.material_id
        , ppf.plant_code
        , ppf.material_desc
        , min(ppf.mm_minimum_lot_size) as mm_min_lot_size
        , ppf.business_unit
      from global_temp."""+TablePrefix+"""planning_parameter_fact ppf
      inner join global_temp."""+TablePrefix+"""active_agreement_fact agf
        on ppf.agreement_naturalkey = agf.agreement_naturalkey
        and ppf.purchase_vendor_id = agf.purchase_vendor_id
        and ppf.plant_code = agf.plant_code
        and ppf.material_id = agf.material_id
      where agf.sbm_enabled_flag like "Y"
      group by 
        ppf.purchase_vendor_id 
        , ppf.material_id
        , ppf.plant_code
        , ppf.material_desc
        , ppf.business_unit
      ),

      min_lot_mat as (

        select 
          ml.vendor_id
          ,ml.material_id
          ,ml.material_desc
          ,ml.mm_min_lot_size
          ,ppf.source_system_code
          ,ml.plant_code
          ,md.base_unit_of_measure as base_unit_of_measure
          ,ppf.business_unit as business_unit
          ,ppf.plant_region_override as region
        from min_lot ml
        inner join global_temp."""+TablePrefix+"""vw_sbm_material_dim md
          on md.material_number = ml.material_id
        inner join global_temp."""+TablePrefix+"""planning_parameter_fact ppf
          on ppf.purchase_vendor_id = ml.vendor_id
          and ppf.material_id = ml.material_id
          --and ppf.material_desc = ml.material_desc
          and ppf.mm_minimum_lot_size = ml.mm_min_lot_size
          and ppf.business_unit = ml.business_unit
      ),

      shelf_life_temp as (
          SELECT material_id, 
                 source_system_code, 
                 material_type,
                 CASE WHEN MIN(total_shelf_life) = 0 THEN NULL ELSE MIN(total_shelf_life) END AS shelf_life
          FROM global_temp."""+TablePrefix+"""mara_sl
          GROUP BY material_id, source_system_code, material_type
      ),

      shelf_life_temp_2 as (

          SELECT material_id, 
                 source_system_code, 
                 material_type,
                 case when (shelf_life is null) or (length(ltrim(rtrim(shelf_life))) = 0) or (cast(shelf_life as int) = 0 ) then '' else cast(shelf_life as varchar(10)) end as material_shelf_life_months
          FROM shelf_life_temp
      ),

      planning_fact_shelf as (

      select 
        mlm.vendor_id
        ,mlm.material_id
        ,mlm.material_desc
        ,mlm.mm_min_lot_size
        ,mlm.plant_code
        ,slt2.material_shelf_life_months
        ,sum(ifnull(cff.sum_total_requirement_qty,'0')) as qty_forecast_n12
        ,mlm.base_unit_of_measure
        ,concat_ws('|', sort_array(collect_set(mlm.business_unit))) as business_unit
        ,concat_ws('|', sort_array(collect_set(mlm.region))) as region
      from min_lot_mat mlm
      left join global_temp."""+TablePrefix+"""consumption_fcst_fact cff
        on cff.material_id = mlm.material_id
        --and cff.purchase_vendor_id = mlm.vendor_id
        and cff.plant_code = mlm.plant_code
        and substring(cff.requirement_date_for_the_component, 0,6) >= concat(cast(DATE_FORMAT(cast(getdate() as date), "yyyy") as int), DATE_FORMAT(cast(getdate() as date), "MM"))
        and substring(cff.requirement_date_for_the_component, 0,6) <= concat(cast(DATE_FORMAT(cast(getdate() as date), "yyyy") + 1 as int), DATE_FORMAT(cast(getdate() as date), "MM"))

      left join shelf_life_temp_2 slt2
        on mlm.material_id = slt2.material_id
        and substring(mlm.source_system_code, 1,3) = substring(slt2.source_system_code, 5,3)
      
      
      group by 
        mlm.vendor_id
        ,mlm.material_id
        ,mlm.material_desc
        ,mlm.mm_min_lot_size
        ,mlm.plant_code
        ,slt2.material_shelf_life_months
        ,mlm.base_unit_of_measure
      )


      select distinct
        cast(vendor_id as int) as vendor_id
        , cast(material_id as int) as material_id
        , material_desc
        , plant_code
        , '' as material_id_phase_in
        , '' as date_phase_in
        , qty_forecast_n12
        , base_unit_of_measure
        , mm_min_lot_size
        , '' as min_prod_qty
        , '' as abc_mat_fam_name
        , '' as restriction_min_prod_qty
        , '' as restriction_max_prod_qty
        , '' as material_per_pallet_qty
        , material_shelf_life_months
        ,business_unit
        ,region
      from planning_fact_shelf

      """
  df_Supplier_Input = spark.sql(sql)
  
  sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start)
except Exception as ex:
  sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

try:
  now_start = datetime.now()
  title = 'export_jaggaer_view_GenerateCSV'
  
  #Below code splits the files
  files = sbm_Function_v2.splitDFtoCSV(df_Supplier_Input, "/mnt/temp/sbm/", "abc_supplier_template", 100000)
  print(files)

  #Below code SFTP transfers the split files list from above
  transfered_files = sbm_Function_v2.transferFiles(files, 'IntoPlatform/abc_supplier_template', JaggaerXferSFTPHost, JaggaerXferSFTPuser, JaggaerXferSFTPpswd, 'put', jaggaer_rsa_key)
  print(transfered_files)

  sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start)
except Exception as ex:
  sbm_Function_v2.sendMessageWithDF("", "Error found", 'Error found: </br></br> Error generating and uploading the abc_supplier_template csv file...</br>'+title+ '</br>'+NotebookName+'. </br></br>'+str(ex), title )
  pythonCustomExceptionHandler(ex, "Error generating and uploading the abc_supplier_template csv file...")  
  sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)

# COMMAND ----------

try:
  title = "Notebook_Reg"
  
  sbm_Function_v2.saveTable('abc_supplier_template', 'parquet', 'overwrite', df_Supplier_Input)
  
  sbm_Function_v2.captureLog('dbx', title, '', '', '1', '0', now_start, '1')
except Exception as ex:
  sbm_Function_v2.captureLog('dbx', title, '', '', '0', '0', now_start, ex)  

# COMMAND ----------

dbutils.notebook.exit(notebookReturnSuccess)
