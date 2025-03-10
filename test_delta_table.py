import pyspark
from delta import *
from delta.tables import *
from pyspark.sql.functions import asc

builder = pyspark.sql.SparkSession.builder.appName("test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

table_path = "raw_opendata_demo"

# Load the Delta table
deltaTable = DeltaTable.forPath(spark, table_path)
fullHistoryDF = deltaTable.history()
fullHistoryDF.show()

#TODO: chek if versions 0 and 1 exist

# Load the DataFrames for versions 0 and 1
df_version_0 = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
df_version_1 = spark.read.format("delta").option("versionAsOf", 1).load(table_path)

# Get the records inserted between version 0 and version 1
new_records = df_version_1.subtract(df_version_0)

# Show the new records
new_records.orderBy(["provincia", "municipio"], ascending=[False, False]).show()


"""
+-----------+-----------------+-------+-----+----------+                        
|  provincia|        municipio|   sexo|total| load_date|
+-----------+-----------------+-------+-----+----------+
|   42 Soria|     42097 Gormaz|Hombres|    5|2025-03-10|
|33 Asturias|    33068 Somiedo|Hombres|  250|2025-03-10|
|33 Asturias|33056 Ribadesella|Hombres|  660|2025-03-10|
|02 Albacete|   02003 Albacete|Hombres|1.935|2025-03-10|
|02 Albacete|   02003 Albacete|Mujeres|1.890|2025-03-10|
+-----------+-----------------+-------+-----+----------+
"""
