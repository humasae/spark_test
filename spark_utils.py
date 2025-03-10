import pyspark
from delta import *
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, expr

builder = pyspark.sql.SparkSession.builder.appName("test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

root_path = "."

# INPUT Functions

def get_dataframe_from_file(input, file_path):
    """
    This function creates a new dataframe from file based on the input variable parameters.
    It loops over spark_options to add dataframe options dinamically
    """
    csvdf = spark.read.format(input.config.get("format"))
    for key,value in input.spark_options.items():
        csvdf = csvdf.option(key, value)
    csvdf = csvdf.load(file_path)
    return csvdf

# TRANSFORMATION Functions

def add_fields_to_dataframe(df, fields):
    """
    This function checks if there are any literal (searching for any single quote) in fields before adding any column,
    using lit or expr functions
    """
    for field in fields:
        if "'" in field["expression"]:
            df_new_fields = df.withColumn(field["name"], lit(field["expression"]))
        else:
            df_new_fields = df.withColumn(field["name"], expr(field["expression"]))
    return df_new_fields

# OUTPUT Functions

def generate_output_file(df, output):
    """
    Generate output file, looking for any partition to be applied in output config
    """
    df = df.write.format(output.config["format"]) \
    .mode(output.config["save_mode"])
    if("partition") in output.config:
        df = df.partitionBy(output.config["partition"])
    df.save(f"./{output.config["path"]}")

def generate_delta_table_with_merge(df, output):
    """
    Create table if it doesn't exist or merge with previous one
    """
    tmp_path = "tmp/delta-table"
    if not DeltaTable.isDeltaTable(spark, tmp_path):
        df.repartition(1).write.format("delta").save(tmp_path)
    else:
        previous_table = DeltaTable.forPath(spark, tmp_path)
        
        merge_keys = ""
        for key in output.config["primary_key"]:
            if not len(merge_keys) == 0:
                merge_keys = merge_keys + " and "
            merge_keys = f"{merge_keys} target.{key} = source.{key}"


        previous_table.alias("target").merge(
            df.alias("source"), merge_keys
        ).execute



def process_dataflows(dataflows, year):
    """
    This function loops over dataflows in order to:
        - create a dataframe from file, given the input instructions
        - apply dataframe transformations, as add fields or apply filters
        - generate json and delta tables, from output specifications

    It creates a dictionary (df_dict) to add the processed dataframes (original, columns added, filters applied)
    """
    for dataf in dataflows:

        df_dict = {}
        
        for input in dataf.inputs:
            if input.type == "file":
                final_csv_path = root_path + f"{(input.config.get("path")).replace('{{ year }}', year)}"
                df_dict[input.name] = get_dataframe_from_file(input, final_csv_path)

        for trans in dataf.transformations:
            if "fields" in trans.config:
                fields = trans.config["fields"]
                # add a new dataframe to dataframe dictionary with the new columns
                df_dict[trans.name] = add_fields_to_dataframe(df_dict[trans.input], fields)

            if "filter" in trans.config:
                filter = trans.config["filter"]
                # recover df with the two aditional columns previously created 
                df_new_filter = df_dict[trans.input].filter(filter)
                # add a new dataframe to dataframe dictionary with the filter applied
                df_dict[trans.name] = df_new_filter
                        
        for output in dataf.outputs:
            if(output.type == "file"):
                df = df_dict[output.input]
                generate_output_file(df, output)

            elif (output.type == "delta"):
                if output.config["save_mode"] == "merge" and ("primary_key") in output.config:
                    df = df_dict[output.input]
                    generate_delta_table_with_merge(df, output)
                    
                else:
                    df = df_dict[output.input]
                    df.repartition(1).write.format("delta")\
                    .mode(output.config["save_mode"])\
                    .save(f"{output.config["table"]}")
