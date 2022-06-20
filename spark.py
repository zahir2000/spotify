#!/usr/bin/python3

#sudo apt install python3-pip
#sudo apt-get install libjpeg-dev zlib1g-dev
#sudo pip3 install Pillow
#import pip

#def import_or_install(package):
#    try:
#        __import__(package)
#    except ImportError:
#        pip.main(['install', package])

#import_or_install('pyspark')
#import_or_install('seaborn')
#import_or_install('matplotlib')

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
import seaborn as sns
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from timeit import default_timer as timer

start = timer()
spark = (SparkSession.builder.config("spark.driver.memory","4g").config("spark.driver.maxResultSize", "4g").getOrCreate())

df = spark.read.csv(path='/home/student/Downloads/charts.csv', inferSchema=True, header=True)

df = df.withColumn("rank", f.col("rank").cast(t.LongType())).withColumn("date", f.col("date").cast(t.DateType())).withColumn("streams", f.col("streams").cast(t.IntegerType()))
df.registerTempTable("charts")
end = timer()
print('\nTime Taken for Data Loading: {:.4f}s\n'.format(end - start))

#===A1===
start = timer()
top10artists = spark.sql('''
SELECT artist, count(artist) as num
FROM charts 
WHERE chart = 'top200'
GROUP BY artist
ORDER BY num DESC
LIMIT 10;
''').toPandas()

print(top10artists)
end = timer()
print('\nTime Taken: {:.4f}s\n'.format(end - start))

#===A2===
start = timer()
top10streams = spark.sql('''
SELECT artist, SUM(streams) streams 
FROM charts 
WHERE streams IS NOT NULL 
GROUP BY artist 
ORDER BY streams DESC 
LIMIT 10;
''').toPandas()

print(top10streams)
end = timer()
print('\nTime Taken: {:.4f}s\n'.format(end - start))

#===A3===
#The no. of days Ed Sheeran songs stayed in their highest rank in 2020.
start = timer()
ed = spark.sql('''
SELECT title, MIN(rank) rank, count(rank) days 
FROM charts 
WHERE artist LIKE '%Ed Sheeran%' 
AND chart = 'top200' 
AND date LIKE '%2020%'
GROUP BY title
ORDER BY rank asc, days desc
LIMIT 10;
''').toPandas()

print(ed)
end = timer()
print('\nTime Taken: {:.4f}s\n'.format(end - start))

#===A4===
#Highest, Lowest and the Average rank of Ed Sheeran Songs
start = timer()
ed2 = spark.sql('''
SELECT Title, MIN(rank) HighestRank, MAX(rank) LowestRank, AVG(rank) AverageRank, COUNT(rank) DaysInCharts
FROM charts 
WHERE artist like '%Ed Sheeran%' 
AND chart='top200' 
GROUP BY title 
ORDER BY AverageRank
LIMIT 10;
''').toPandas()

print(ed2)
end = timer()
print('\nTime Taken: {:.4f}s\n'.format(end - start))

#===A5===
#Top Countries Ed Sheeran Charted In The Most
start = timer()
ed3 = spark.sql('''
SELECT region, count(region) days
FROM charts
WHERE artist like '%Ed Sheeran%'
AND chart='top200'
GROUP BY region
ORDER BY days desc
LIMIT 10;
''').toPandas()

print(ed3)
end = timer()
print('\nTime Taken: {:.4f}s\n'.format(end - start))
