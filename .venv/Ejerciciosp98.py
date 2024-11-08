from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Fire Incidents Analysis").getOrCreate()

data = spark.read.json("data/iot_devices.json")

df = spark.createDataFrame(data.rdd)

df.show(10)

# 1. Detect failing devices with battery levels below a threshold.

#(df.select("device_name","battery_level").where(col("battery_level")<1).show(20))

# 2. Identify offending countries with high levels of CO2 emissions.

#df.select("c02_level", "cn").distinct().orderBy(col("c02_level").desc()).show(20)


# 3. Compute the min and max values for temperature, battery level, CO2, and humidity.

df.agg(
    max("temp").alias("MAX_TEMP"),
    min("temp").alias("MIN_TEMP"),
    max("battery_level").alias("MAX_BL"),
    min("battery_level").alias("MIN_BL"),
    max("c02_level").alias("MAX_c02"),
    min("c02_level").alias("MIN_c02"),
    max("humidity").alias("MAX_HUM"),
    min("humidity").alias("MIN_HUM")
).show()



# 4. Sort and group by average temperature, CO2, humidity, and country


df.groupBy("cn").agg(
    avg("temp").alias("AVG_TEMP"),
    avg("c02_level").alias("AVG_CO2"),
    avg("humidity").alias("AVG_HUM")
).orderBy(avg("temp").desc()).show(truncate=False)


spark.stop()