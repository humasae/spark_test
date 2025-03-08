import pyspark
from delta import *
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, expr
import sys
import json



#spark = SparkSession.builder.appName("test").getOrCreate()

builder = pyspark.sql.SparkSession.builder.appName("test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

base_path = "files"


class JsonItemClass:
    name: str
    type: str
    config: dict

    def __init__(self, name, type, config):
        self.type = type
        self.config = config
        self.name = name

class InputClass(JsonItemClass):
    spark_options: dict [str, str]

    def __init__(self, name, type, config, spark_options):
        self.name = name
        self.type = type
        self.config = config
        self.spark_options = spark_options  



class DataflowClass:
    inputs: list[JsonItemClass]
    transformations: list[JsonItemClass]
    outputs: list[JsonItemClass]


# Read JSON with json library

with open("metadata.json") as f:
    metadata_json = json.load(f)

##########################

dataflows = []

for dataflow in metadata_json["dataflows"]:
    dataf = DataflowClass
    # Read inputs
    inputs = []
    for input in dataflow['inputs']:
        obj_input = InputClass(input["name"], input["type"], input["config"], input["spark_options"])
        inputs.append(obj_input)
    dataf.inputs = inputs

    # Read transformations
    transformations = []
    for transformation in dataflow["transformations"]:
        obj_trans = JsonItemClass(transformation["name"], transformation["type"], transformation["config"])
        transformations.append(obj_trans)
    dataf.transformations = transformations

    #Read outputs
    outputs = []
    for output in dataflow["outputs"]:
        obj_output = JsonItemClass(output["name"], output["type"], output["config"])
        outputs.append(obj_output)
    dataf.outputs = outputs
    
    dataflows.append(dataf)


# # create dataframe from csv
root_path = "."
if len(sys.argv) > 1:
    year = sys.argv[1]
else:
    year = "2025"
df_list = []


for dataf in dataflows:
    for input in dataf.inputs:
        if input.type == "file":
            final_csv_path = root_path + f"{(input.config.get("path")).replace('{{ year }}', year)}"
            csvdf = spark.read.format(input.config.get("format"))
            for key,value in input.spark_options.items():
                csvdf = csvdf.option(key, value)
            csvdf = csvdf.load(final_csv_path)
            
            df_list.append(csvdf)

    for trans in dataf.transformations:
        if "fields" in trans.config:
            fields = trans.config["fields"]
            for field in fields:
                for index,df in enumerate(df_list):
                    if "'" in field["expression"]:
                        df_list[index] = df.withColumn(field["name"], lit(field["expression"]))
                    else:
                        df_list[index] = df.withColumn(field["name"], expr(field["expression"]))

        if "filter" in trans.config:
            filter = trans.config["filter"]
            for field in fields:
                for index,df in enumerate(df_list):
                    df_list[index] = df.filter(filter)
                    
    for output in dataf.outputs:
        if(output.type == "file"):
            for df in df_list:
                df = df.write.format(output.config["format"]) \
                .mode(output.config["save_mode"])
                if("partition") in output.config:
                    df = df.partitionBy(output.config["partition"])
                df.save(f"./{output.config["path"]}")

        elif (output.type == "delta"):
            if output.config["save_mode"] == "merge" and ("primary_key") in output.config:
                
                for df in df_list:
                    if not DeltaTable.isDeltaTable(spark, "tmp/delta-table"):
                        df.repartition(1).write.format("delta").save("tmp/delta-table")
                    else:
                        previous_table = DeltaTable.forPath(spark, "tmp/delta-table")
                        
                        merge_keys = ""
                        for key in output.config["primary_key"]:
                            if not len(merge_keys) == 0:
                                merge_keys = merge_keys + " and "
                            merge_keys = f"{merge_keys} target.{key} = source.{key}"


                        previous_table.alias("target").merge(
                            df.alias("source"), merge_keys
                        ).execute
            else:
                for df in df_list:
                    df.write.format("delta")\
                    .mode(output.config["save_mode"])\
                    .saveAsTable(output.config["table"])
            

for df in df_list:
    df.printSchema()
    df.show(n=10)




    