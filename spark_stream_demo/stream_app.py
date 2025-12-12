from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("LocalStructuredStreaming") \
    .master("local[*]") \
    .getOrCreate()

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.readStream.schema(schema).json("input/")

query = df.writeStream \
    .format("json")\
    .option("path", "output/")\
    .option("checkpointLocation", "checkpoint/")\
    .outputMode("append")\
    .start()



query.awaitTermination()


