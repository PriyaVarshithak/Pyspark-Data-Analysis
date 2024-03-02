# -*- coding: utf-8 -*-

!pip install pyspark

import pyspark
from pyspark.sql import SparkSession
# Create a Spark Session
spark = SparkSession.builder.master("local[*]").getOrCreate()

from pyspark.sql.functions import *
from pyspark.sql.types import *
#2.1.creating a dataframe in pyspark using csv
df = spark.read.option("header",True).csv("/content/sample_data/train.csv")
# df.printSchema()
df.show(10)

#2.2.filling nan values with empty string
df1=df.na.fill(value='')
df1.show(10)

#2.3.calculating average age of people survived
df2=df1.groupBy("Survived").agg(avg("Age").alias("SurAggAge"))
df2.show()
#Assuming survived people as 1
df_final=df2.select("Survived","SurAggAge").where(col("Survived")==1)
df_final.show()

#2.4.printing even numbers from a list using parallelize
sparkContext=spark.sparkContext
def EvenNumbers(rangen):
  rangenum= sparkContext.parallelize(range(rangen[0],rangen[1]+1))
  df3=rangenum.filter(lambda n:(n%2==0))
  for n in df3.collect():
    print(n)
EvenNumbers([1,100])
