from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, expr
import sys
import json


spark = SparkSession.builder.appName("test").getOrCreate()

base_path = "files"


class InputClass:
    name: str
    type: str
    config: dict[str, str]
    spark_options: dict [str, str]

    def __init__(self, name, type, config, spark_options):
        self.name = name
        self.type = type
        self.config = config
        self.spark_options = spark_options    

    def get_path(self):
        return(self.config.get("path"))

    def get_format(self):
        return(f"{self.config.get("format")}")
    
class TransformationClass:
    type: str
    config: dict

    def __init__(self, type, config):
        self.type = type
        self.config = config



class DataflowClass:
    inputs: list[InputClass]
    transformations: list[TransformationClass]


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
        obj_trans = TransformationClass(transformation["type"], transformation["config"])
        transformations.append(obj_trans)
    dataf.transformations = transformations
    
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
            final_csv_path = root_path + f"{(input.get_path()).replace('{{ year }}', year)}"
            csvdf = spark.read.format(input.get_format())
            for key,value in input.spark_options.items():
                csvdf = csvdf.option(key, value)
            csvdf = csvdf.load(final_csv_path)
            csvdf.withColumn("patata", lit('demography'))

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
                    


for df in df_list:
    df.printSchema()
    df.show(n=10)


# data = [('James','Smith','M',3000), ('Anna','Rose','F',4100),
#   ('Robert','Williams','M',6200)
# ]
# columns = ["firstname","lastname","gender","salary"]
# df = spark.createDataFrame(data=data, schema = columns)
# df.show()

# df.withColumn("bonus_percent", lit(0.3)) \
#   .show()
# # Add New column with NULL
# df.withColumn("DEFAULT_COL", lit(None)) \
#   .show()

# df.withColumn("load_date", current_date()) \
#   .show()

# df.show()



    