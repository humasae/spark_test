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
    input: str

    def __init__(self, name, type, input, config):
        self.type = type
        self.config = config
        self.input = input
        self.name = name

class InputClass(JsonItemClass):
    spark_options: dict [str, str]

    def __init__(self, name, type, config, spark_options):
        self.name = name
        self.type = type
        self.config = config
        self.spark_options = spark_options  

class DataflowClass:
    name: str
    inputs: list[InputClass]
    transformations: list[JsonItemClass]
    outputs: list[JsonItemClass]


def get_json(json_path):
    """
    Read JSON with json library
    """
    with open(json_path) as f:
        return json.load(f)
    
def get_dataflows_from_json(metadata_json):
    dataflows = []
    
    for dataflow in metadata_json["dataflows"]:
        dataf = DataflowClass
        dataf.name = dataflow["name"]
        # Read inputs
        inputs = []
        for input in dataflow["inputs"]:
            obj_input = InputClass(input["name"], input["type"], input["config"], input["spark_options"])
            inputs.append(obj_input)
        dataf.inputs = inputs

        # Read transformations
        transformations = []
        for transformation in dataflow["transformations"]:
            obj_trans = JsonItemClass(transformation["name"], transformation["type"], transformation["input"], transformation["config"])
            transformations.append(obj_trans)
        dataf.transformations = transformations

        #Read outputs
        outputs = []
        for output in dataflow["outputs"]:
            obj_output = JsonItemClass(output["name"], output["type"], output["input"], output["config"])
            outputs.append(obj_output)
        dataf.outputs = outputs
        
        dataflows.append(dataf)
    return dataflows
    
def process_dataflows(dataflows, year):
    for dataf in dataflows:
        df_dict = {}
        for input in dataf.inputs:
            if input.type == "file":
                final_csv_path = root_path + f"{(input.config.get("path")).replace('{{ year }}', year)}"
                csvdf = spark.read.format(input.config.get("format"))
                for key,value in input.spark_options.items():
                    csvdf = csvdf.option(key, value)
                csvdf = csvdf.load(final_csv_path)

                df_dict[input.name] = csvdf

        for trans in dataf.transformations:
            if "fields" in trans.config:
                fields = trans.config["fields"]
                for field in fields:
                    if "'" in field["expression"]:
                        df_new_fields = df_dict[trans.input].withColumn(field["name"], lit(field["expression"]))
                    else:
                        df_new_fields = df_dict[trans.input].withColumn(field["name"], expr(field["expression"]))
                df_dict[trans.name] = df_new_fields

            if "filter" in trans.config:
                filter = trans.config["filter"]
                for field in fields:
                    df_new_filter = df_dict[trans.input].filter(filter)
                df_dict[trans.name] = df_new_filter
                        
        for output in dataf.outputs:
            if(output.type == "file"):
                df = df_dict[output.input]
                df = df.write.format(output.config["format"]) \
                .mode(output.config["save_mode"])
                if("partition") in output.config:
                    df = df.partitionBy(output.config["partition"])
                df.save(f"./{output.config["path"]}")

            elif (output.type == "delta"):
                if output.config["save_mode"] == "merge" and ("primary_key") in output.config:
                    
                    df = df_dict[output.input]
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
                    df = df_dict[output.input]
                    df.repartition(1).write.format("delta")\
                    .mode(output.config["save_mode"])\
                    .save(f"{output.config["table"]}")

                    dfprev = spark.read.format("delta").load(f"{output.config["table"]}")
                    dfprev.show()


if __name__ == "__main__":
    metadata_json = get_json("metadata.json")
    dataflows = get_dataflows_from_json(metadata_json)

    # # create dataframe from csv
    root_path = "."
    if len(sys.argv) > 1:
        year = sys.argv[1]
    else:
        year = "2025"

    process_dataflows(dataflows, year)


    

    





    