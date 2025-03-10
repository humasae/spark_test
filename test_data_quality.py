from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

spark = SparkSession.builder.appName("test").getOrCreate()

# TODO: pass path as parameter

csv_path = "./data/input/opendata_demo/poblacion2025.csv"
csv_df = spark.read.format("csv")\
.option("header", "true")\
.option("delimiter", ";")\
.load(csv_path)

csv_df = csv_df.withColumn("total_integer", col("total").cast("int"))

csv_df.show(n=10)

grouped_df = csv_df.groupBy("sexo").agg(sum("total_integer"), count("total_integer"))

# show the results
grouped_df.show()

"""
2024
+-----------+------------------+--------------------+
|       sexo|sum(total_integer)|count(total_integer)|
+-----------+------------------+--------------------+
|Ambos sexos|            689296|                7756|
|    Mujeres|            509675|                7756|
|    Hombres|            494565|                7756|
+-----------+------------------+--------------------+

2025
+-----------+------------------+--------------------+
|       sexo|sum(total_integer)|count(total_integer)|
+-----------+------------------+--------------------+
|Ambos sexos|            689293|                7756|
|    Mujeres|            509675|                7756|
|    Hombres|            494565|                7756|
+-----------+------------------+--------------------+

"""