from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# from pyspark.sql.connect.functions import concat
# import pandas
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# import pyarrow
# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .config('spark.sql.shuffle.partitions',3)\
      .config('spark.sql.adaptive.enabled','false')\
      .getOrCreate()

df = spark.read.format('csv').option('header','true')\
    .option('inferSchema','true')\
    .option('columnNameOfCorruptRecord','_corrupt_record')\
    .option('path','D:\\Apache_Spark\\csv\\adi.csv').load()
# df = spark.read.format('json').option("multiline", "true")\
#     .option('mode','DROPMALFORMED')\
#     .option('path',"D:\\Apache_Spark\\json\\multiline_wrong.json").load()
df.show()

input('Press any key to exit')