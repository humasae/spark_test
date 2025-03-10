from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, regexp_replace, lit

spark = SparkSession.builder.appName("test").getOrCreate()

# TODO: pass path as parameter

csv_path = "./data/input/opendata_demo/poblacion2025.csv"
csv_df = spark.read.format("csv")\
.option("header", "true")\
.option("delimiter", ";")\
.load(csv_path)

csv_df = csv_df.withColumn("total_integer", regexp_replace(col("total"), r'\.', '').cast("int"))
csv_df.createOrReplaceTempView("population")

grouped_df_both_sex = csv_df.filter(col("sexo")=="Ambos sexos").groupBy("municipio")\
    .agg(sum("total_integer").alias("total_both_sex")).orderBy("municipio")
grouped_df_men_women = csv_df.filter(col("sexo")!="Ambos sexos").groupBy("municipio")\
    .agg(sum("total_integer").alias("total_men_women")).orderBy("municipio")

joined_df = grouped_df_both_sex.join(grouped_df_men_women, on="municipio")
diff_df = joined_df.withColumn("difference", col("total_both_sex") - col("total_men_women"))

result_df = diff_df.select("municipio", "total_both_sex", "total_men_women", "difference").filter(col("difference")!=0)
result_df.show()

"""
+--------------+--------------+---------------+----------+
|     municipio|total_both_sex|total_men_women|difference|
+--------------+--------------+---------------+----------+
|  42097 Gormaz|            12|             14|        -2|
|02003 Albacete|          3827|           3825|         2|
+--------------+--------------+---------------+----------+
"""