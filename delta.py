# Databricks notebook source
# MAGIC %run ../helpers/multi_id_generator

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame

# COMMAND ----------

spark = SparkSession.builder.appName("Legacy - Delta Load")\
        .getOrCreate()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "314572800")

process_name = 'Legacy ID Platform'

# COMMAND ----------

try:
    ## Define file paths
    tbl_id_input_path = dbutils.widgets.get("s3_tbl_ids_path_delta")
    tbl_changeHist_input_path = dbutils.widgets.get("s3_tbl_changehistory_path")
    tbl_changeHistbvd9_input_path = dbutils.widgets.get("s3_tbl_changehistory_bvd9_path")
    hist_bit_arc_input_path = dbutils.widgets.get("s3_histo_bit_archive_path")
    availability_input_path = dbutils.widgets.get("s3_tbl_availability_path")
    product_input_path = dbutils.widgets.get("s3_tbl_products_path")


    ## Read files into dataframe
    tbl_id_df = spark.read.option("header", "true").parquet(tbl_id_input_path)
    tbl_changeHist_df = spark.read.option("header", "true").parquet(tbl_changeHist_input_path)
    tbl_changeHistbvd9_df = spark.read.option("header", "true").parquet(tbl_changeHistbvd9_input_path)
    hist_bit_arc_df = spark.read.option("header", "true").parquet(hist_bit_arc_input_path)
    availability_df = spark.read.option("header", "true").parquet(availability_input_path)
    product_df = spark.read.option("header", "true").parquet(product_input_path)

    ## Read Postgress Table columns for final dataframe schema
    org_xref_pos_cols = postgres_read_tbl(f'{config.schema_name}.organization_xref', process_name).columns
    org_pos_cols = postgres_read_tbl(f'{config.schema_name}.organization', process_name).columns
    entity_pos_cols = postgres_read_tbl(f'{config.schema_name}.entity', process_name).columns
    org_xref_hist_pos_cols = postgres_read_tbl(f'{config.schema_name}.organization_xref_history', process_name).columns

    org_xref_pos_tempcols = postgres_read_tbl(f'{config.schema_name}.organization_xref_legacy_temp', process_name).columns
    org_pos_tempcols = postgres_read_tbl(f'{config.schema_name}.organization_legacy_temp', process_name).columns

    ## Picking latest Row_ID from Histo Bit Archive file
    window_spec = Window.partitionBy("Row_ID").orderBy(col("date_bit_changed").desc())
    hist_bit_arc_df1 = hist_bit_arc_df.withColumn("row_number", row_number().over(window_spec))
    hist_bit_arc_df2 = hist_bit_arc_df1.filter(col("row_number") == 1).drop("row_number")

    ## Assigning alias names for the dataframe
    changeHist_df_alias = tbl_changeHist_df.alias("changeHist")
    changeHistbvd9_df_alias = tbl_changeHistbvd9_df.alias("changeHistbvd9")
    histBitArc_df_alias = hist_bit_arc_df2.alias("histBitArc")
    availability_df_alias = availability_df.alias("availability")
    product_df_alias = product_df.alias("product")
    

    logger.info("File Read Completed Successfully")
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while reading files and tables")
    logger.error(log_msg)
    sys.exit() 

# COMMAND ----------

## Renaming columns as per the target table schema
tbl_id_df_renamed = tbl_id_df.withColumnRenamed('Row_ID', 'row_id')\
                        .withColumnRenamed('Product_ID', 'product_id')\
                        .withColumnRenamed('isbvdId', 'is_bvd_id')\
                        .withColumnRenamed('CompleteId', 'initial_bvd_id')\
                        .withColumn('initial_orbis_id', lpad('InitialBvd9', 9, '0'))\
                        .withColumn('surviving_orbis_id', lpad('Unique_ID', 9, '0'))\
                        .withColumn("matched_date", to_date(col('MatchingDate').cast("string"), 'yyyyMMdd'))\
                        .withColumn("indexation_tag", to_date(col('Indexation_Tag').cast("string"), 'yyyyMMdd'))\
                        .withColumn("deleted",  col("Deleted").cast('string') )\
                        .drop('Local_ID', 'Iso2', 'MatchingDate', 'Unique_ID', 'InitialBvd9')

# COMMAND ----------

## Function to generate surviving_bvd_id and other columns :: change_type parameter can be 'insert' or 'update'
def generate_columns(df, process_name, change_type):
    tbl_id_df3 = generate_surviving_bvd_id(df, process_name)

    ## Join with Availability to populate account_id and bvd_id_status columns

    tbl_id_df3_alias = tbl_id_df3.alias("tbl_id3")
    tbl_id3_cols = [f"tbl_id3.{col}" for col in tbl_id_df3.columns]

    tbl_id_df4 = tbl_id_df3_alias.join(availability_df_alias, tbl_id_df3_alias["row_id"] == availability_df_alias["Row_ID"], how="left")\
                        .select(*tbl_id3_cols, "availability.Archived", "availability.Accounts", col("availability.Row_ID").alias("availability_id"))
                            
    tbl_id_df5 = tbl_id_df4.withColumn('bvd_id_status', when((col("availability_id").isNotNull()) & (col("Archived") == 0), "available")
                                                    .when((col("availability_id").isNotNull()) & (col("Archived") == 1), "histo")
                                                    .when((col("availability_id").isNotNull()) & (col("Archived") == 2), "ghost histo(removed histo)")
                                                    .otherwise("not available"))\
                        .withColumnRenamed('Accounts', 'account_id')\
                        .drop('Archived', 'availability_id')


    ## Join with Product to populate product_priority

    tbl_id_df5_alias = tbl_id_df5.alias("tbl_id5")
    tbl_id5_cols = [f"tbl_id5.{col}" for col in tbl_id_df5.columns]

    tbl_id_df6 = tbl_id_df5_alias.join(broadcast(product_df_alias), tbl_id_df5_alias["product_id"] == product_df_alias["Product_ID"], how="left")\
                            .select(*tbl_id5_cols, col("product.Product_Priority").alias("product_priority") )


    ## Join with Change History to populate comment_chg_hist and blacklisted_chg_hist columns

    tbl_id_df6_alias = tbl_id_df6.alias("tbl_id6")
    tbl_id6_cols = [f"tbl_id6.{col}" for col in tbl_id_df6.columns]

    tbl_id_df7 = tbl_id_df6_alias.join(changeHist_df_alias, (tbl_id_df6_alias["initial_bvd_id"] == changeHist_df_alias["OldId"])
                                    & (tbl_id_df6_alias["surviving_bvd_id"] == changeHist_df_alias["NewId"]), how="left")\
                                .select(*tbl_id6_cols, "changeHist.Comments", "changeHist.Blacklisted")
    tbl_id_df7 = tbl_id_df7.withColumnRenamed('Blacklisted', 'blacklisted_chg_hist')\
                            .withColumnRenamed('Comments', 'comment_chg_hist')


    ## Join with Change History BVD9 to populate comment_chg_hist_bvd9 and blacklisted_chg_hist_bvd9 columns

    tbl_id_df7_alias = tbl_id_df7.alias("tbl_id7")
    tbl_id7_cols = [f"tbl_id7.{col}" for col in tbl_id_df7.columns]

    tbl_id_df8 = tbl_id_df7_alias.join(changeHistbvd9_df_alias, (tbl_id_df7_alias["initial_bvd_id"] == changeHistbvd9_df_alias["OldUniqueId"])
                                    & (tbl_id_df7_alias["surviving_bvd_id"] == changeHistbvd9_df_alias["NewUniqueId"]), how="left")\
                                .select(*tbl_id7_cols, "changeHistbvd9.Comment", "changeHistbvd9.BlacklistFlag")
    tbl_id_df8 = tbl_id_df8.withColumnRenamed('BlacklistFlag', 'blacklisted_chg_hist_bvd9')\
                            .withColumnRenamed('Comment', 'comment_chg_hist_bvd9')


    ## Join with Histo Bit Archive to populate last_closing_date and date_bit_changed columns

    tbl_id_df8_alias = tbl_id_df8.alias("tbl_id8")
    tbl_id8_cols = [f"tbl_id8.{col}" for col in tbl_id_df8.columns]   

    tbl_id_df9 = tbl_id_df8_alias.join(histBitArc_df_alias, tbl_id_df8_alias["row_id"] == histBitArc_df_alias["Row_ID"], how="left")\
                                .select(*tbl_id8_cols, "histBitArc.LastClosingDate", "histBitArc.date_bit_changed")
    tbl_id_df_final = tbl_id_df9.withColumn('date_bit_changed1', to_date(col('date_bit_changed')))\
                            .withColumn('last_closing_date',  when((col("LastClosingDate") == 0) | (col("LastClosingDate").isNull()), lit(None).cast("string"))
                                                        .otherwise(concat_ws('-', substring(col("LastClosingDate"), 1, 4), substring(col("LastClosingDate"), 5, 2), substring(col("LastClosingDate"), 7, 2))))\
                            .drop('LastClosingDate', 'date_bit_changed')\
                            .withColumnRenamed('date_bit_changed1', 'date_bit_changed')\
                            .withColumnRenamed('row_id', 'availability_id')


    ## Populating audit columns and other column values with NULL values
    if change_type == 'insert':                                           
        org_xref_df = tbl_id_df_final.withColumn('organization_row_id', md5(concat_ws('', tbl_id_df_final['surviving_universal_id'])))\
                                .withColumn('universal_id_status', lit('confirmed'))\
                                .withColumn('initial_bvdliens_id', lit(None).cast('string'))\
                                .withColumn('surviving_bvdliens_id', lit(None).cast('string'))\
                                .withColumn('bms_match_status', lit(None).cast('string'))\
                                .withColumn('comment_chg_hist_universal', lit(None).cast('string'))\
                                .withColumn('comment_universal_chg_date', lit(None).cast('date'))\
                                .withColumn('source_nbr', lit(None).cast('int'))\
                                .withColumn('entity_exclusion_flag', lit(None).cast('string'))\
                                .withColumn('advisor_link_flag', lit(None).cast('int'))\
                                .withColumn('private_equity_flag', lit(None).cast('int'))\
                                .withColumn('branch_flag', lit(None).cast('int'))\
                                .withColumn('foreign_entity_flag', lit(None).cast('int'))\
                                .withColumn('headquarter_flag', lit(None).cast('int'))\
                                .withColumn('listed_flag', lit(None).cast('int'))\
                                .withColumn('last_transfer_date', lit(None).cast('string'))\
                                .withColumn('last_account_date', lit(None).cast('string'))\
                                .withColumn('operating_revenue', lit(None).cast('int'))\
                                .withColumn('total_asset', lit(None).cast('int'))\
                                .withColumn('requestor_email_id', lit(None).cast('string'))\
                                .withColumn("org_sub_type",  lit(None).cast('string'))\
                                .withColumn("created_by", lit("system"))\
                                .withColumn("created_date", current_timestamp())\
                                .withColumn("updated_by", lit("system"))\
                                .withColumn("updated_date", current_timestamp())\
                                .withColumn("surviving_universal_id_text", lit(None).cast('string'))
                                
        org_xref_df_final = org_xref_df.withColumn('organization_xref_row_id', md5(concat_ws('', org_xref_df['initial_universal_id'], org_xref_df['initial_orbis_id'], org_xref_df['initial_bvd_id'], org_xref_df['product_id'])))\
                .select(*org_xref_pos_cols)\
                .drop('surviving_universal_id_text')\
                .repartition(400, 'organization_xref_row_id')

        org_df = org_xref_df_final.filter(col("is_bvd_id") == 'Y')\
                                .withColumnRenamed('surviving_universal_id', 'universal_id')\
                                .withColumnRenamed('surviving_orbis_id', 'orbis_id')\
                                .withColumnRenamed('surviving_bvd_id', 'bvd_id')\
                                .withColumnRenamed('surviving_bvdliens_id', 'bvdliens_id')\
                                .select(*org_pos_cols).repartition(250, 'organization_row_id')   


        entity_df = org_df.select('universal_id', 'created_by', 'created_date', 'updated_by', 'updated_date').distinct()\
                        .withColumn('entity_type_id', lit('L'))\
                        .withColumn("source", lit("legacy_id_platform"))\
                        .select(*entity_pos_cols).repartition(250, 'universal_id') 

        return org_xref_df_final, org_df, entity_df
    else:
        org_xref_df_final = tbl_id_df_final.select(*org_xref_pos_tempcols).repartition(400, 'organization_xref_row_id')

        org_df = tbl_id_df_final.filter(col("is_bvd_id") == 'Y')\
                                .withColumnRenamed('surviving_universal_id', 'universal_id')\
                                .withColumnRenamed('surviving_orbis_id', 'orbis_id')\
                                .withColumnRenamed('surviving_bvd_id', 'bvd_id')\
                                .withColumnRenamed('surviving_bvdliens_id', 'bvdliens_id')\
                                .select(*org_pos_tempcols).repartition(250, 'organization_row_id')
                                
        return org_xref_df_final, org_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete Logic

# COMMAND ----------

try:
    timestamp = datetime.now()
    ## Filter out records with change_type as delete
    tbl_id_df_delete = tbl_id_df_renamed.filter(col('change_type') == 'delete')

    if tbl_id_df_delete.count() > 0:
        tbl_id_df_delete1 = tbl_id_df_delete.withColumn('delete_records', concat_ws('-', col('product_id'), col('initial_bvd_id')))

        ## Get a list of concat of product_id and initial_bvd_id
        orgxref_delete_list = tuple([str(row) for row in tbl_id_df_delete1.select(f.collect_list('delete_records')).first()[0]])

        ## Query to get and update records from organization_xref table with the list of product_id and initial_bvd_id
        if len(orgxref_delete_list) == 1:
            orgxref_delete_list = orgxref_delete_list[0]
            select_delete_query = f"select a.* from (select *, concat_ws('-', product_id, initial_bvd_id) as delete_records from {config.schema_name}.organization_xref where universal_id_status = 'confirmed') a where a.delete_records = '{orgxref_delete_list}' "

            orgxref_inactive_status_query = f"update {config.schema_name}.organization_xref set universal_id_status = 'inactive', updated_date = '{timestamp}' where universal_id_status = 'confirmed' and concat_ws('-', product_id, initial_bvd_id)  = '{orgxref_delete_list}' "
        else:
            select_delete_query = f"select a.* from (select *, concat_ws('-', product_id, initial_bvd_id) as delete_records from {config.schema_name}.organization_xref where universal_id_status = 'confirmed') a where a.delete_records in {orgxref_delete_list}"

            orgxref_inactive_status_query = f"update {config.schema_name}.organization_xref set universal_id_status = 'inactive', updated_date = '{timestamp}' where universal_id_status = 'confirmed' and concat_ws('-', product_id, initial_bvd_id) in {orgxref_delete_list}"

        select_delete_data = postgres_read_qry(select_delete_query, process_name)

        ## Filter on golden records and update as Inactive in Organization Table
        org_delete_list = tuple([row for row in select_delete_data.filter(col('is_bvd_id') == 'Y').select('surviving_universal_id').select(f.collect_list('surviving_universal_id')).first()[0]])

        if len(org_delete_list) == 1:
            logger.info("Updating in Organization table as inactive")
            org_delete_list = org_delete_list[0]
            org_inactive_status_query = f"update {config.schema_name}.organization set universal_id_status = 'inactive', updated_date = '{timestamp}' where universal_id_status = 'confirmed' and universal_id = '{org_delete_list}' "
            execute_nonselect_query(org_inactive_status_query, process_name, f"{config.schema_name}.organization")
        elif len(org_delete_list) > 1:
            logger.info("Updating in Organization table as inactive")
            org_inactive_status_query = f"update {config.schema_name}.organization set universal_id_status = 'inactive', updated_date = '{timestamp}' where universal_id_status = 'confirmed' and universal_id in {org_delete_list} "
            execute_nonselect_query(org_inactive_status_query, process_name, f"{config.schema_name}.organization")
        else:
            logger.info("No records to update in Organization table")
            pass
        
        ## Get Records to move to Organization_xref_history Table
        org_xref_hist = select_delete_data.withColumn("hist_entry_date", current_timestamp())
        org_xref_hist1 = org_xref_hist.withColumn('organization_xref_hist_row_id', md5(concat_ws('', org_xref_hist['organization_xref_row_id'], org_xref_hist['hist_entry_date'])))\
                                    .drop('surviving_universal_id_text', 'delete_records')\
                                    .select(*org_xref_hist_pos_cols)

        logger.info("Moving delete records from Organization to Organization_xref_history table")
        postgres_write_tbl(org_xref_hist1, 'append', f'{config.schema_name}.organization_xref_history', process_name)

        ## Make Records as Inactive in Organozation_xref Table
        logger.info("Updating in Organization_xref table as inactive")
        execute_nonselect_query(orgxref_inactive_status_query, process_name, f"{config.schema_name}.organization_xref")

    else:
        logger.info("No Delete Records Found")
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while processing delete logic on tables")
    logger.error(log_msg)
    sys.exit()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert Logic : New Orbis ID

# COMMAND ----------

## Filter out records with change_type as insert with on-demand
tbl_id_df_on_demand = tbl_id_df_renamed.filter( (col('change_type') == 'insert') & (col('initial_bvd_id').like('Multi%')) )

# COMMAND ----------

## Filter out records with change_type as insert without on-demand
tbl_id_df_insert = tbl_id_df_renamed.filter( (col('change_type') == 'insert') & (~col('initial_bvd_id').like('Multi%')) )

try:
    if tbl_id_df_insert.isEmpty():
        insert = False
        true_insert = False
        logger.info("No Insert Records Found")
        insert_orbis_notpresent_df =spark.createDataFrame([], StructType([]))
        insert_orbis_present_df = spark.createDataFrame([], StructType([]))
        insert_present_orbis_list =[]
    else:
        insert = True
        ## Filter out list of orbis ids from the delta and search in organization_xref table to identify which are completely new orbis ids in the file
        insert_orbis_list = tuple([str(row) for row in tbl_id_df_insert.select('surviving_orbis_id').distinct().select(f.collect_list('surviving_orbis_id')).first()[0]])

        insert_xref_orbis_present_query = f"select a.* from {config.schema_name}.organization_xref a where a.universal_id_status = 'confirmed' and a.surviving_orbis_id in {insert_orbis_list}"
        insert_xref_orbis_present_data = postgres_read_qry(insert_xref_orbis_present_query, process_name)

        insert_xref_orbis_list = [str(row) for row in insert_xref_orbis_present_data.select('surviving_orbis_id').distinct().select(f.collect_list('surviving_orbis_id')).first()[0]]

        insert_orbis_notpresent_df = tbl_id_df_insert.filter(~col('surviving_orbis_id').isin(insert_xref_orbis_list))
        if insert_orbis_notpresent_df.isEmpty():
            true_insert = False
            logger.info("No Insert Records With New OrbisID Found") 
        else:
            true_insert = True
            ## Generate universal ids to the new records
            insert_orbis_notpresent_df1, max_multiid_sequence, consumed_sequence_count, sequence_part_used = multi_id_generator(insert_orbis_notpresent_df, 'surviving_orbis_id', process_name)
            insert_orbis_notpresent_df2 = insert_orbis_notpresent_df1.withColumn("surviving_universal_id", col("initial_universal_id"))

            insert_orbis_notpresent_org_xref, insert_orbis_notpresent_org, insert_orbis_notpresent_entity = generate_columns(insert_orbis_notpresent_df2, process_name, 'insert')
            
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while processing insert records with Orbis ID")
    logger.error(log_msg)
    sys.exit()  


# COMMAND ----------

## Load new records to organization_xref, organozation and entity table
try:
    if true_insert == True:
        postgres_write_tbl(insert_orbis_notpresent_org_xref, 'append', f'{config.schema_name}.organization_xref', process_name)
        postgres_write_tbl(insert_orbis_notpresent_org, 'append', f'{config.schema_name}.organization', process_name)
        postgres_write_tbl(insert_orbis_notpresent_entity, 'append', f'{config.schema_name}.entity', process_name)
    else:
        pass
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while writing to postgres for insert records with Orbis ID")
    logger.error(log_msg)
    sys.exit()

if true_insert == True:
    update_status_completed(sequence_part_used, max_multiid_sequence, consumed_sequence_count, process_name)
else:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert Logic : Existing Orbis ID

# COMMAND ----------

try:
    if insert == False:
        pass  
    else:
        insert_xref_orbis_present_query1 = f"select a.*, concat_ws('-', a.product_id, a.initial_bvd_id) as key_concat from {config.schema_name}.organization_xref a where a.universal_id_status = 'confirmed' and a.surviving_orbis_id in {insert_orbis_list}"
        insert_xref_orbis_present_data1 = postgres_read_qry(insert_xref_orbis_present_query1, process_name)

        insert_xref_key_concat_list = [str(row) for row in insert_xref_orbis_present_data1.select('key_concat').distinct().select(f.collect_list('key_concat')).first()[0]]

        tbl_id_df_insert_concat = tbl_id_df_insert.withColumn('key_concat', f.concat_ws('-', col('product_id'), col('initial_bvd_id')))
        insert_orbis_present_df = tbl_id_df_insert_concat.filter((col('surviving_orbis_id').isin(insert_xref_orbis_list)) & (~col('key_concat').isin(insert_xref_key_concat_list)))
        insert_present_orbis_list = [str(row) for row in insert_orbis_present_df.select(f.collect_list('surviving_orbis_id')).first()[0]]
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while processing insert records with existing Orbis ID")
    logger.error(log_msg)
    sys.exit()  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Logic

# COMMAND ----------

tbl_id_df_update = tbl_id_df_renamed.filter(col('change_type') == 'update')

try:
    if tbl_id_df_update.isEmpty():
        update = False
        logger.info("No Update Records Found")
        update_key_list = []
        update_orbis_list = []
        update_prev_orbis_list = []
        update_orbis_present_df = spark.createDataFrame([], StructType([]))
        update_orbis_notpresent_df = spark.createDataFrame([], StructType([]))

    else:
        update = True
        tbl_id_df_update1 = tbl_id_df_update.withColumn('key_concat', concat_ws('-', col('product_id'), col('initial_bvd_id')))

        update_orbis_list = [str(row) for row in tbl_id_df_update1.select(f.collect_list('surviving_orbis_id')).first()[0]]
        update_key_list = tuple([str(row) for row in tbl_id_df_update1.select(f.collect_list('key_concat')).first()[0]])
        
        update_xref_query = f"select distinct a.surviving_orbis_id from (select *, concat_ws('-', product_id, initial_bvd_id) as key_concat from {config.schema_name}.organization_xref) a where a.universal_id_status = 'confirmed' and a.key_concat in {update_key_list}"
        update_xref_data = postgres_read_qry(update_xref_query, process_name)

        update_prev_orbis_list = [str(row) for row in update_xref_data.select(f.collect_list('surviving_orbis_id')).first()[0]]
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while processing update records")
    logger.error(log_msg)
    sys.exit() 

# COMMAND ----------

try:
    prehist_orbis_list = tuple(set(insert_present_orbis_list).union(set(update_orbis_list)).union(set(update_prev_orbis_list)))

    prehist_query =  f"select * from {config.schema_name}.organization_xref where universal_id_status = 'confirmed' and surviving_orbis_id in {prehist_orbis_list}"
    prehist_data = postgres_read_qry(prehist_query, process_name)

    postgres_write_tbl(prehist_data, 'overwrite', f'{config.schema_name}.organization_xref_legacy_prehist_temp', process_name)

    prehist_data_repart = prehist_data.repartition(100, 'organization_xref_row_id')
    prehist_data_repart.cache()
    prehist_data_repart.limit(1).count()
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while writing previous records into temp table for update records")
    logger.error(log_msg)
    sys.exit() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert Logic : Existing Orbis ID - Fetch Universal IDs and generate other columns

# COMMAND ----------

try:
    if insert_orbis_present_df.isEmpty():
        insert_orbis_present_df2 = spark.createDataFrame([], StructType([]))
        logger.info("No Insert Records Found with existing Orbis ID")      
    else:
        prehist_data_repart_distinct = prehist_data_repart.select('initial_universal_id', 'surviving_universal_id', 'surviving_orbis_id').distinct()
        prehist_data_repart_alias = prehist_data_repart_distinct.alias("prehist_data_repart")

        insert_orbis_present_alias = insert_orbis_present_df.alias("insert_orbis_present")
        insert_orbis_present_cols = [f"insert_orbis_present.{col}" for col in insert_orbis_present_df.columns]

        insert_orbis_present_df1 = insert_orbis_present_alias.join(prehist_data_repart_alias,
                                                                insert_orbis_present_alias["surviving_orbis_id"] == prehist_data_repart_alias["surviving_orbis_id"], how="left")\
                                        .select(*insert_orbis_present_cols, 
                                                col("prehist_data_repart.initial_universal_id"),
                                                col("prehist_data_repart.surviving_universal_id"))
        
        insert_orbis_present_df2 = insert_orbis_present_df1.withColumn('organization_xref_row_id', md5(concat_ws('', insert_orbis_present_df1['initial_universal_id'], insert_orbis_present_df1['initial_orbis_id'], insert_orbis_present_df1['initial_bvd_id'], insert_orbis_present_df1['product_id'])))\
                                    .withColumn('organization_row_id', md5(concat_ws('', insert_orbis_present_df1['surviving_universal_id'])))\
                                    .withColumn('universal_id_status', lit('confirmed'))\
                                    .withColumn('initial_bvdliens_id', lit(None).cast('string'))\
                                    .withColumn('surviving_bvdliens_id', lit(None).cast('string'))\
                                    .withColumn('bms_match_status', lit(None).cast('string'))\
                                    .withColumn('comment_chg_hist_universal', lit(None).cast('string'))\
                                    .withColumn('comment_universal_chg_date', lit(None).cast('date'))\
                                    .withColumn('source_nbr', lit(None).cast('int'))\
                                    .withColumn('entity_exclusion_flag', lit(None).cast('string'))\
                                    .withColumn('advisor_link_flag', lit(None).cast('int'))\
                                    .withColumn('private_equity_flag', lit(None).cast('int'))\
                                    .withColumn('branch_flag', lit(None).cast('int'))\
                                    .withColumn('foreign_entity_flag', lit(None).cast('int'))\
                                    .withColumn('headquarter_flag', lit(None).cast('int'))\
                                    .withColumn('listed_flag', lit(None).cast('int'))\
                                    .withColumn('last_transfer_date', lit(None).cast('string'))\
                                    .withColumn('last_account_date', lit(None).cast('string'))\
                                    .withColumn('operating_revenue', lit(None).cast('int'))\
                                    .withColumn('total_asset', lit(None).cast('int'))\
                                    .withColumn('requestor_email_id', lit(None).cast('string'))\
                                    .withColumn("org_sub_type",  lit(None).cast('string'))
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while processing insert records with existing Orbis ID")
    logger.error(log_msg)
    sys.exit()                               

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Logic : Fetch Initial Universal ID and other columns

# COMMAND ----------

try:
    if update == False:
        pass
    else:
        prehist_data_repart_alias = prehist_data_repart.alias("prehist_data_repart")
        tbl_id_df_update_alias = tbl_id_df_update.alias("tbl_id_df_update")
        tbl_id_df_update_cols = [f"tbl_id_df_update.{col}" for col in tbl_id_df_update.columns]

        tbl_id_df_update1 = tbl_id_df_update_alias.join(prehist_data_repart_alias,
                                                            (tbl_id_df_update_alias["product_id"] == prehist_data_repart_alias["product_id"]) &
                                                            (tbl_id_df_update_alias["initial_bvd_id"] == prehist_data_repart_alias["initial_bvd_id"]), how="left")\
                                        .select(*tbl_id_df_update_cols, 
                                                col("prehist_data_repart.initial_universal_id"),
                                                col("prehist_data_repart.organization_xref_row_id"),
                                                col("prehist_data_repart.universal_id_status"),
                                                col("prehist_data_repart.initial_bvdliens_id"),
                                                col("prehist_data_repart.surviving_bvdliens_id"),
                                                col("prehist_data_repart.bms_match_status"),
                                                col("prehist_data_repart.comment_chg_hist_universal"),
                                                col("prehist_data_repart.comment_universal_chg_date"),
                                                col("prehist_data_repart.source_nbr"),
                                                col("prehist_data_repart.entity_exclusion_flag"),
                                                col("prehist_data_repart.advisor_link_flag"),
                                                col("prehist_data_repart.private_equity_flag"),
                                                col("prehist_data_repart.branch_flag"),
                                                col("prehist_data_repart.foreign_entity_flag"),
                                                col("prehist_data_repart.headquarter_flag"),
                                                col("prehist_data_repart.listed_flag"),
                                                col("prehist_data_repart.last_transfer_date"),
                                                col("prehist_data_repart.last_account_date"),
                                                col("prehist_data_repart.operating_revenue"),
                                                col("prehist_data_repart.total_asset"),
                                                col("prehist_data_repart.requestor_email_id"),
                                                col("prehist_data_repart.org_sub_type")
                                            )
                                        
        update_xref_orbis_list = [str(row) for row in prehist_data_repart.select('surviving_orbis_id').filter(col('surviving_orbis_id').isin(update_orbis_list)).distinct().select(f.collect_list('surviving_orbis_id')).first()[0]]


        update_orbis_present_df = tbl_id_df_update1.filter(col('surviving_orbis_id').isin(update_xref_orbis_list))
        update_orbis_notpresent_df = tbl_id_df_update1.filter(~col('surviving_orbis_id').isin(update_xref_orbis_list))
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while processing update records")
    logger.error(log_msg)
    sys.exit()  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Logic : New Orbis ID - Generate Universal IDs

# COMMAND ----------

try:
    if (update == True) and (update_orbis_notpresent_df.isEmpty()):
        update_orbis_notpresent_df2 = spark.createDataFrame([], StructType([]))
        logger.info("No Update Records Found with new Orbis ID")
    else:
        update_orbis_notpresent_df1, max_multiid_sequence, consumed_sequence_count, sequence_part_used = multi_id_generator_legacy_update(update_orbis_notpresent_df, 'surviving_orbis_id', process_name)
        update_orbis_notpresent_df2 = update_orbis_notpresent_df1.withColumn('organization_row_id', md5(concat_ws('', update_orbis_notpresent_df1['surviving_universal_id'])))
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while processing update records with new Orbis ID")
    logger.error(log_msg)
    sys.exit() 
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Logic : Existing Orbis ID - Generate Universal IDs

# COMMAND ----------

try:
    if (update == True) and (update_orbis_present_df.isEmpty()):
        update_orbis_present_df = spark.createDataFrame([], StructType([]))
        logger.info("No Update Records Found with existing OrbisID")
    else:
        update_orbis_present_alias = update_orbis_present_df.alias("update_orbis_present")
        update_orbis_present_cols = [f"update_orbis_present.{col}" for col in update_orbis_present_df.columns]

        update_orbis_present_df1 = update_orbis_present_alias.join(prehist_data_repart_alias,
                                                            update_orbis_present_alias["surviving_orbis_id"] == prehist_data_repart_alias["surviving_orbis_id"], how="left")\
                                        .select(*update_orbis_present_cols, 
                                                col("prehist_data_repart.surviving_universal_id")
                                        )

        update_orbis_present_df2 = update_orbis_present_df1.withColumn('organization_row_id', md5(concat_ws('', update_orbis_present_df1['surviving_universal_id'])))
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while processing update records with existing Orbis ID")
    logger.error(log_msg)
    sys.exit()    


# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine Data and write to temp tables

# COMMAND ----------

try:
    prehist_data_repart_except_new_key = prehist_data_repart.filter(~concat_ws('-', col('product_id'), col('initial_bvd_id')).isin(list(update_key_list)))\
                                                      .withColumnRenamed('availability_id', 'row_id')

    insert_update_merge = insert_orbis_present_df2.unionByName(update_orbis_notpresent_df2, allowMissingColumns=True)\
                                                    .unionByName(update_orbis_present_df2, allowMissingColumns=True)\
                                                      .unionByName(prehist_data_repart_except_new_key, allowMissingColumns=True)\
                                                        .drop('key_concat', 'change_type', 'SequnceNumber', 'created_by', 'created_date', 'updated_by', 'updated_date', 'account_id', 'bvd_id_status', 'product_priority', 'blacklisted_chg_hist', 'comment_chg_hist', 'blacklisted_chg_hist_bvd9', 'comment_chg_hist_bvd9', 'last_closing_date', 'date_bit_changed', 'surviving_universal_id_text', 'surviving_bvd_id')\
                                                          .dropDuplicates()

    update_org_xref, update_org = generate_columns(insert_update_merge, process_name, 'update')
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while processing combine data of insert-update and updates")
    logger.error(log_msg)
    sys.exit()

# COMMAND ----------

try:
    postgres_write_tbl(update_org_xref, 'overwrite', f'{config.schema_name}.organization_xref_legacy_temp', process_name)
    postgres_write_tbl(update_org, 'overwrite', f'{config.schema_name}.organization_legacy_temp', process_name)
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while writing to postgres to temp tables for update records")
    logger.error(log_msg)
    sys.exit()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge into Org Xref and Org tables

# COMMAND ----------

try:
    logger.info("Merge Query Started")
    merge_query = f"""merge into {config.schema_name}.organization_xref t
                    using {config.schema_name}.organization_xref_legacy_temp tmp
                        on t.product_id = tmp.product_id and 
                        t.initial_bvd_id = tmp.initial_bvd_id
                    when not matched  then 
                        insert (organization_xref_row_id,
                            organization_row_id,
                            initial_universal_id,
                            surviving_universal_id,
                            universal_id_status,
                            initial_orbis_id,
                            surviving_orbis_id,
                            initial_bvd_id,
                            surviving_bvd_id,
                            is_bvd_id,
                            bvd_id_status,
                            initial_bvdliens_id,
                            surviving_bvdliens_id,
                            bms_match_status,
                            availability_id,
                            product_id,
                            account_id,
                            product_priority,
                            comment_chg_hist_universal,
                            comment_universal_chg_date,
                            source_nbr,
                            deleted,
                            entity_exclusion_flag,
                            advisor_link_flag,
                            private_equity_flag,
                            branch_flag,
                            foreign_entity_flag,
                            headquarter_flag,
                            listed_flag,
                            blacklisted_chg_hist,
                            blacklisted_chg_hist_bvd9,
                            last_closing_date,
                            date_bit_changed,
                            matched_date,
                            last_transfer_date,
                            last_account_date,
                            comment_chg_hist,
                            comment_chg_hist_bvd9,
                            operating_revenue,
                            total_asset,
                            org_sub_type,
                            indexation_tag,
                            requestor_email_id,
                            created_by,
                            created_date,
                            updated_by,
                            updated_date)
                        values (tmp.organization_xref_row_id,
                            tmp.organization_row_id,
                            tmp.initial_universal_id,
                            tmp.surviving_universal_id,
                            tmp.universal_id_status,
                            tmp.initial_orbis_id,
                            tmp.surviving_orbis_id,
                            tmp.initial_bvd_id,
                            tmp.surviving_bvd_id,
                            tmp.is_bvd_id,
                            tmp.bvd_id_status,
                            tmp.initial_bvdliens_id,
                            tmp.surviving_bvdliens_id,
                            tmp.bms_match_status,
                            tmp.availability_id,
                            tmp.product_id,
                            tmp.account_id,
                            tmp.product_priority,
                            tmp.comment_chg_hist_universal,
                            tmp.comment_universal_chg_date,
                            tmp.source_nbr,
                            tmp.deleted,
                            tmp.entity_exclusion_flag,
                            tmp.advisor_link_flag,
                            tmp.private_equity_flag,
                            tmp.branch_flag,
                            tmp.foreign_entity_flag,
                            tmp.headquarter_flag,
                            tmp.listed_flag,
                            tmp.blacklisted_chg_hist,
                            tmp.blacklisted_chg_hist_bvd9,
                            tmp.last_closing_date,
                            tmp.date_bit_changed,
                            tmp.matched_date,
                            tmp.last_transfer_date,
                            tmp.last_account_date,
                            tmp.comment_chg_hist,
                            tmp.comment_chg_hist_bvd9,
                            tmp.operating_revenue,
                            tmp.total_asset,
                            tmp.org_sub_type,
                            tmp.indexation_tag,
                            tmp.requestor_email_id,
                            'system',
                            current_timestamp,
                            'system',
                            current_timestamp)
                    when matched and concat(t.organization_xref_row_id,
                                        t.organization_row_id,
                                        t.initial_universal_id,
                                        t.surviving_universal_id,
                                        t.universal_id_status,
                                        t.initial_orbis_id,
                                        t.surviving_orbis_id,
                                        t.surviving_bvd_id,
                                        t.is_bvd_id,
                                        t.bvd_id_status,
                                        t.initial_bvdliens_id,
                                        t.surviving_bvdliens_id,
                                        t.bms_match_status,
                                        t.availability_id,
                                        t.account_id,
                                        t.product_priority,
                                        t.comment_chg_hist_universal,
                                        t.comment_universal_chg_date,
                                        t.source_nbr,
                                        t.deleted,
                                        t.entity_exclusion_flag,
                                        t.advisor_link_flag,
                                        t.private_equity_flag,
                                        t.branch_flag,
                                        t.foreign_entity_flag,
                                        t.headquarter_flag,
                                        t.listed_flag,
                                        t.blacklisted_chg_hist,
                                        t.blacklisted_chg_hist_bvd9,
                                        t.last_closing_date,
                                        t.date_bit_changed,
                                        t.matched_date,
                                        t.last_transfer_date,
                                        t.last_account_date,
                                        t.comment_chg_hist,
                                        t.comment_chg_hist_bvd9,
                                        t.operating_revenue,
                                        t.total_asset,
                                        t.org_sub_type,
                                        t.indexation_tag,
                                        t.requestor_email_id)
                                        != 
                                        concat(tmp.organization_xref_row_id,
                                        tmp.organization_row_id,
                                        tmp.initial_universal_id,
                                        tmp.surviving_universal_id,
                                        tmp.universal_id_status,
                                        tmp.initial_orbis_id,
                                        tmp.surviving_orbis_id,
                                        tmp.surviving_bvd_id,
                                        tmp.is_bvd_id,
                                        tmp.bvd_id_status,
                                        tmp.initial_bvdliens_id,
                                        tmp.surviving_bvdliens_id,
                                        tmp.bms_match_status,
                                        tmp.availability_id,
                                        tmp.account_id,
                                        tmp.product_priority,
                                        tmp.comment_chg_hist_universal,
                                        tmp.comment_universal_chg_date,
                                        tmp.source_nbr,
                                        tmp.deleted,
                                        tmp.entity_exclusion_flag,
                                        tmp.advisor_link_flag,
                                        tmp.private_equity_flag,
                                        tmp.branch_flag,
                                        tmp.foreign_entity_flag,
                                        tmp.headquarter_flag,
                                        tmp.listed_flag,
                                        tmp.blacklisted_chg_hist,
                                        tmp.blacklisted_chg_hist_bvd9,
                                        tmp.last_closing_date,
                                        tmp.date_bit_changed,
                                        tmp.matched_date,
                                        tmp.last_transfer_date,
                                        tmp.last_account_date,
                                        tmp.comment_chg_hist,
                                        tmp.comment_chg_hist_bvd9,
                                        tmp.operating_revenue,
                                        tmp.total_asset,
                                        tmp.org_sub_type,
                                        tmp.indexation_tag,
                                        tmp.requestor_email_id) then 
                        update set
                        organization_xref_row_id = tmp.organization_xref_row_id,
                        organization_row_id = tmp.organization_row_id,
                        initial_universal_id = tmp.initial_universal_id,
                        surviving_universal_id = tmp.surviving_universal_id,
                        universal_id_status = tmp.universal_id_status,
                        initial_orbis_id = tmp.initial_orbis_id,
                        surviving_orbis_id = tmp.surviving_orbis_id,
                        initial_bvd_id = tmp.initial_bvd_id,
                        surviving_bvd_id = tmp.surviving_bvd_id,
                        is_bvd_id = tmp.is_bvd_id,
                        bvd_id_status = tmp.bvd_id_status,
                        initial_bvdliens_id = tmp.initial_bvdliens_id,
                        surviving_bvdliens_id = tmp.surviving_bvdliens_id,
                        bms_match_status = tmp.bms_match_status,
                        availability_id = tmp.availability_id,
                        product_id = tmp.product_id,
                        account_id = tmp.account_id,
                        product_priority = tmp.product_priority,
                        comment_chg_hist_universal = tmp.comment_chg_hist_universal,
                        comment_universal_chg_date = tmp.comment_universal_chg_date,
                        source_nbr = tmp.source_nbr,
                        deleted = tmp.deleted,
                        entity_exclusion_flag = tmp.entity_exclusion_flag,
                        advisor_link_flag = tmp.advisor_link_flag,
                        private_equity_flag = tmp.private_equity_flag,
                        branch_flag = tmp.branch_flag,
                        foreign_entity_flag = tmp.foreign_entity_flag,
                        headquarter_flag = tmp.headquarter_flag,
                        listed_flag = tmp.listed_flag,
                        blacklisted_chg_hist = tmp.blacklisted_chg_hist,
                        blacklisted_chg_hist_bvd9 = tmp.blacklisted_chg_hist_bvd9,
                        last_closing_date = tmp.last_closing_date,
                        date_bit_changed = tmp.date_bit_changed,
                        matched_date = tmp.matched_date,
                        last_transfer_date = tmp.last_transfer_date,
                        last_account_date = tmp.last_account_date,
                        comment_chg_hist = tmp.comment_chg_hist,
                        comment_chg_hist_bvd9 = tmp.comment_chg_hist_bvd9,
                        operating_revenue = tmp.operating_revenue,
                        total_asset = tmp.total_asset,
                        org_sub_type = tmp.org_sub_type,
                        indexation_tag = tmp.indexation_tag,
                        requestor_email_id = tmp.requestor_email_id,
                        created_by = t.created_by,
                        created_date = t.created_date,
                        updated_by = 'system',
                        updated_date = current_timestamp ;"""

    execute_nonselect_query(merge_query, process_name, f"{config.schema_name}.organization_xref")
    logger.info("Merge Query Executed")
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while upserting from temp to organization_xref table")
    logger.error(log_msg)
    sys.exit()

# COMMAND ----------

try:
    logger.info("Merge Query Started")
    merge_query = f"""merge into {config.schema_name}.organization t
                    using {config.schema_name}.organization_legacy_temp tmp
                        on t.universal_id = tmp.universal_id 
                    when not matched  then 
                        insert (organization_row_id,
                            universal_id,
                            universal_id_status,
                            orbis_id,
                            bvd_id,
                            is_bvd_id,
                            bvd_id_status,
                            bvdliens_id,
                            bms_match_status,
                            availability_id,
                            product_id,
                            account_id,
                            product_priority,
                            comment_chg_hist_universal,
                            comment_universal_chg_date,
                            source_nbr,
                            deleted,
                            entity_exclusion_flag,
                            advisor_link_flag,
                            private_equity_flag,
                            branch_flag,
                            foreign_entity_flag,
                            headquarter_flag,
                            listed_flag,
                            blacklisted_chg_hist,
                            blacklisted_chg_hist_bvd9,
                            last_closing_date,
                            date_bit_changed,
                            matched_date,
                            last_transfer_date,
                            last_account_date,
                            comment_chg_hist,
                            comment_chg_hist_bvd9,
                            operating_revenue,
                            total_asset,
                            org_sub_type,
                            indexation_tag,
                            requestor_email_id,
                            created_by,
                            created_date,
                            updated_by,
                            updated_date)
                        values (tmp.organization_row_id,
                            tmp.universal_id,
                            tmp.universal_id_status,
                            tmp.orbis_id,
                            tmp.bvd_id,
                            tmp.is_bvd_id,
                            tmp.bvd_id_status,
                            tmp.bvdliens_id,
                            tmp.bms_match_status,
                            tmp.availability_id,
                            tmp.product_id,
                            tmp.account_id,
                            tmp.product_priority,
                            tmp.comment_chg_hist_universal,
                            tmp.comment_universal_chg_date,
                            tmp.source_nbr,
                            tmp.deleted,
                            tmp.entity_exclusion_flag,
                            tmp.advisor_link_flag,
                            tmp.private_equity_flag,
                            tmp.branch_flag,
                            tmp.foreign_entity_flag,
                            tmp.headquarter_flag,
                            tmp.listed_flag,
                            tmp.blacklisted_chg_hist,
                            tmp.blacklisted_chg_hist_bvd9,
                            tmp.last_closing_date,
                            tmp.date_bit_changed,
                            tmp.matched_date,
                            tmp.last_transfer_date,
                            tmp.last_account_date,
                            tmp.comment_chg_hist,
                            tmp.comment_chg_hist_bvd9,
                            tmp.operating_revenue,
                            tmp.total_asset,
                            tmp.org_sub_type,
                            tmp.indexation_tag,
                            tmp.requestor_email_id,
                            'system',
                            current_timestamp,
                            'system',
                            current_timestamp)
                    when matched and concat(t.organization_row_id,
                                    t.universal_id_status,
                                    t.orbis_id,
                                    t.bvd_id,
                                    t.is_bvd_id,
                                    t.bvd_id_status,
                                    t.bvdliens_id,
                                    t.bms_match_status,
                                    t.availability_id,
                                    t.product_id,
                                    t.account_id,
                                    t.product_priority,
                                    t.comment_chg_hist_universal,
                                    t.comment_universal_chg_date,
                                    t.source_nbr,
                                    t.deleted,
                                    t.entity_exclusion_flag,
                                    t.advisor_link_flag,
                                    t.private_equity_flag,
                                    t.branch_flag,
                                    t.foreign_entity_flag,
                                    t.headquarter_flag,
                                    t.listed_flag,
                                    t.blacklisted_chg_hist,
                                    t.blacklisted_chg_hist_bvd9,
                                    t.last_closing_date,
                                    t.date_bit_changed,
                                    t.matched_date,
                                    t.last_transfer_date,
                                    t.last_account_date,
                                    t.comment_chg_hist,
                                    t.comment_chg_hist_bvd9,
                                    t.operating_revenue,
                                    t.total_asset,
                                    t.org_sub_type,
                                    t.indexation_tag,
                                    t.requestor_email_id)
                                    != 
                                    concat(tmp.organization_row_id,
                                    tmp.universal_id_status,
                                    tmp.orbis_id,
                                    tmp.bvd_id,
                                    tmp.is_bvd_id,
                                    tmp.bvd_id_status,
                                    tmp.bvdliens_id,
                                    tmp.bms_match_status,
                                    tmp.availability_id,
                                    tmp.product_id,
                                    tmp.account_id,
                                    tmp.product_priority,
                                    tmp.comment_chg_hist_universal,
                                    tmp.comment_universal_chg_date,
                                    tmp.source_nbr,
                                    tmp.deleted,
                                    tmp.entity_exclusion_flag,
                                    tmp.advisor_link_flag,
                                    tmp.private_equity_flag,
                                    tmp.branch_flag,
                                    tmp.foreign_entity_flag,
                                    tmp.headquarter_flag,
                                    tmp.listed_flag,
                                    tmp.blacklisted_chg_hist,
                                    tmp.blacklisted_chg_hist_bvd9,
                                    tmp.last_closing_date,
                                    tmp.date_bit_changed,
                                    tmp.matched_date,
                                    tmp.last_transfer_date,
                                    tmp.last_account_date,
                                    tmp.comment_chg_hist,
                                    tmp.comment_chg_hist_bvd9,
                                    tmp.operating_revenue,
                                    tmp.total_asset,
                                    tmp.org_sub_type,
                                    tmp.indexation_tag,
                                    tmp.requestor_email_id) then 
                        update set
                        organization_row_id = tmp.organization_row_id,
                        universal_id = tmp.universal_id,
                        universal_id_status = tmp.universal_id_status,
                        orbis_id = tmp.orbis_id,
                        bvd_id = tmp.bvd_id,
                        is_bvd_id = tmp.is_bvd_id,
                        bvd_id_status = tmp.bvd_id_status,
                        bvdliens_id = tmp.bvdliens_id,
                        bms_match_status = tmp.bms_match_status,
                        availability_id = tmp.availability_id,
                        product_id = tmp.product_id,
                        account_id = tmp.account_id,
                        product_priority = tmp.product_priority,
                        comment_chg_hist_universal = tmp.comment_chg_hist_universal,
                        comment_universal_chg_date = tmp.comment_universal_chg_date,
                        source_nbr = tmp.source_nbr,
                        deleted = tmp.deleted,
                        entity_exclusion_flag = tmp.entity_exclusion_flag,
                        advisor_link_flag = tmp.advisor_link_flag,
                        private_equity_flag = tmp.private_equity_flag,
                        branch_flag = tmp.branch_flag,
                        foreign_entity_flag = tmp.foreign_entity_flag,
                        headquarter_flag = tmp.headquarter_flag,
                        listed_flag = tmp.listed_flag,
                        blacklisted_chg_hist = tmp.blacklisted_chg_hist,
                        blacklisted_chg_hist_bvd9 = tmp.blacklisted_chg_hist_bvd9,
                        last_closing_date = tmp.last_closing_date,
                        date_bit_changed = tmp.date_bit_changed,
                        matched_date = tmp.matched_date,
                        last_transfer_date = tmp.last_transfer_date,
                        last_account_date = tmp.last_account_date,
                        comment_chg_hist = tmp.comment_chg_hist,
                        comment_chg_hist_bvd9 = tmp.comment_chg_hist_bvd9,
                        operating_revenue = tmp.operating_revenue,
                        total_asset = tmp.total_asset,
                        org_sub_type = tmp.org_sub_type,
                        indexation_tag = tmp.indexation_tag,
                        requestor_email_id = tmp.requestor_email_id,
                        created_by = t.created_by,
                        created_date = t.created_date,
                        updated_by = 'system',
                        updated_date = current_timestamp ;"""

    execute_nonselect_query(merge_query, process_name, f"{config.schema_name}.organization")
    logger.info("Merge Query Executed")
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while upserting from temp to organization table")
    logger.error(log_msg)
    sys.exit()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Moving data to Org Xref History table

# COMMAND ----------

try:
    update_org_xref.createOrReplaceTempView("update_org_xref_vw")
    prehist_data_repart.createOrReplaceTempView("prehist_data_repart_vw")

    hist_data = spark.sql(""" select t1.* from prehist_data_repart_vw t1 
                          inner join update_org_xref_vw t2
                          on t1.product_id = t2.product_id and 
                          t1.initial_bvd_id = t2.initial_bvd_id 
                          where 
                          t1.organization_xref_row_id != t2.organization_xref_row_id or
                          t1.organization_row_id != t2.organization_row_id or
                          t1.initial_universal_id != t2.initial_universal_id or
                          t1.surviving_universal_id != t2.surviving_universal_id or
                          t1.universal_id_status != t2.universal_id_status or
                          t1.initial_orbis_id != t2.initial_orbis_id or
                          t1.surviving_orbis_id != t2.surviving_orbis_id or
                          t1.surviving_bvd_id != t2.surviving_bvd_id or
                          t1.is_bvd_id != t2.is_bvd_id or
                          t1.bvd_id_status != t2.bvd_id_status or
                          t1.initial_bvdliens_id != t2.initial_bvdliens_id or
                          t1.surviving_bvdliens_id != t2.surviving_bvdliens_id or
                          t1.bms_match_status != t2.bms_match_status or
                          t1.availability_id != t2.availability_id or
                          t1.account_id != t2.account_id or
                          t1.product_priority != t2.product_priority or
                          t1.comment_chg_hist_universal != t2.comment_chg_hist_universal or
                          t1.comment_universal_chg_date != t2.comment_universal_chg_date or
                          t1.source_nbr != t2.source_nbr or
                          t1.deleted != t2.deleted or
                          t1.entity_exclusion_flag != t2.entity_exclusion_flag or
                          t1.advisor_link_flag != t2.advisor_link_flag or
                          t1.private_equity_flag != t2.private_equity_flag or
                          t1.branch_flag != t2.branch_flag or
                          t1.foreign_entity_flag != t2.foreign_entity_flag or
                          t1.headquarter_flag != t2.headquarter_flag or
                          t1.listed_flag != t2.listed_flag or
                          t1.blacklisted_chg_hist != t2.blacklisted_chg_hist or
                          t1.blacklisted_chg_hist_bvd9 != t2.blacklisted_chg_hist_bvd9 or
                          t1.last_closing_date != t2.last_closing_date or
                          t1.date_bit_changed != t2.date_bit_changed or
                          t1.matched_date != t2.matched_date or
                          t1.last_transfer_date != t2.last_transfer_date or
                          t1.last_account_date != t2.last_account_date or
                          t1.comment_chg_hist != t2.comment_chg_hist or
                          t1.comment_chg_hist_bvd9 != t2.comment_chg_hist_bvd9 or
                          t1.operating_revenue != t2.operating_revenue or
                          t1.total_asset != t2.total_asset or
                          t1.org_sub_type != t2.org_sub_type or
                          t1.indexation_tag != t2.indexation_tag or
                          t1.requestor_email_id != t2.requestor_email_id  """)

    hist_data1 = hist_data.withColumn("hist_entry_date", current_timestamp())
    hist_data_final = hist_data1.withColumn('organization_xref_hist_row_id', md5(concat_ws('', hist_data1['organization_xref_row_id'], hist_data1['hist_entry_date'])))\
                                .drop('surviving_universal_id_text')\
                                .select(*org_xref_hist_pos_cols)
    postgres_write_tbl(hist_data_final, 'append', f'{config.schema_name}.organization_xref_history', process_name)
except Exception as e:
    log_msg= format_logging_msg(source=process_name, message= e, code_reference="Error while writing to org xreg history posgres table")
    logger.error(log_msg)
    sys.exit()

# COMMAND ----------

update_status_completed(sequence_part_used, max_multiid_sequence, consumed_sequence_count, process_name)
