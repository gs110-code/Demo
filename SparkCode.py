from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql.types import * 
from pyspark.sql import HiveContext


spark = SparkSession.builder.appName("IMDB_Spark_Normalized").enableHiveSupport().getOrCreate()

sqlContext = HiveContext(spark)



schema = StructType([ \
					StructField("sno", StringType(), True), \
					StructField("title", StringType(), True), \
					StructField("year", StringType(), True), \
				    StructField("kind", StringType(), True), \
					StructField("genre", StringType(), True), \
					StructField("rating", StringType(), True), \
					StructField("vote", StringType(), True), \
					StructField("country", StringType(), True), \
					StructField("language", StringType(), True), \
					StructField("runtime", StringType(), True), \
					StructField("casts", StringType(), True), \
					StructField("director", StringType(), True), \
					StructField("composer", StringType(), True), \
					StructField("writer", StringType(), True), \
					StructField("runtimes", StringType(), True)
					])


imdb_file_name = spark.read.schema(schema) \
    .option("delimiter",",") \
    .option("header","true") \
	.option("skiprow",1) \
    .csv("s3://big-data-batch01/guri/pyspark/")
	
def extract_date(str):
      date = str.split("imdb_")[1].split(".")[0].replace('_','-')
      return date
	
extract_date_value = udf(lambda x: extract_date(x))
	

file_date = imdb_file_name.withColumn("filedate", extract_date_value(input_file_name()).cast("date"))	  
explode_genre = file_date.withColumn('explode_gen', split(file_date['genre'],','))
explode_country = explode_genre.withColumn('explode_countries', split(explode_genre['country'],','))

df_explode_genre = explode_country.select(explode_country.sno,explode_country.title, explode(explode_country.explode_gen))
df_explode_country = explode_country.select(explode_country.sno,explode_country.title, explode(explode_country.explode_countries))


	
explode_country.write.mode("Overwrite") \
         .partitionBy("filedate") \
         .saveAsTable("imdb_spark.imdb_spark_normalized")
		 
