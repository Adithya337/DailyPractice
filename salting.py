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

id_values1 = [1]*100000+[2]*5+[3]*2
df1 = spark.createDataFrame(id_values1,"int").toDF('id')
# df1 = df1.repartition(3)
id_values2 = [1]*100+[2]*5+[3]*2
df2 = spark.createDataFrame(id_values2,"int").toDF('id')
# df2 = df2.repartition(3)
print(df1.rdd.getNumPartitions())
print(df2.rdd.getNumPartitions())
# print(df1.count())
# print(df2.count())


df3 = df1.join(df2,df1['id']==df2['id'],'inner')
# df3.show()
print(df3.count())

df1 = df1.withColumn('random_num',(rand()*10+1).cast('int'))
df1 = df1.withColumn('salted_key',concat(('id'),lit('_'),expr('random_num'))).drop('random_num')
df2 = df2.withColumn('sequence',array([lit(i) for i in range(1,11)]))
df2 = df2.withColumn('exploded_col',explode(col('sequence')))\
      .withColumn('salted_key',concat(expr('id'),lit('_'),expr('exploded_col'))).drop('exploded_col','sequence')
# df2.show()
df3 = df1.join(df2,df2['salted_key']==df1['salted_key'],'inner')
# df3.show()
print(df3.count())
input('Press any key to exit')