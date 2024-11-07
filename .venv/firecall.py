from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Crear una sesión de Spark
spark = SparkSession.builder.appName("Fire Incidents Analysis").getOrCreate()

# Definir el esquema programáticamente
fire_schema = StructType([
    StructField('IncidentNumber', StringType(), True),
    StructField('ExposureNumber', StringType(), True),
    StructField('ID', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('IncidentDate', StringType(), True),
    StructField('CallNumber', StringType(), True),
    StructField('AlarmDtTm', StringType(), True),
    StructField('ArrivalDtTm', StringType(), True),
    StructField('CloseDtTm', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', StringType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('SuppressionUnits', StringType(), True),
    StructField('SuppressionPersonnel', StringType(), True),
    StructField('EMSUnits', StringType(), True),
    StructField('EMSPersonnel', StringType(), True),
    StructField('OtherUnits', StringType(), True),
    StructField('OtherPersonnel', StringType(), True),
    StructField('FirstUnitOnScene', StringType(), True),
    StructField('EstimatedPropertyLoss', StringType(), True),
    StructField('EstimatedContentsLoss', StringType(), True),
    StructField('FireFatalities', StringType(), True),
    StructField('FireInjuries', StringType(), True),
    StructField('CivilianFatalities', StringType(), True),
    StructField('CivilianInjuries', StringType(), True),
    StructField('NumberOfAlarms', StringType(), True),
    StructField('PrimarySituation', StringType(), True),
    StructField('MutualAid', StringType(), True),
    StructField('ActionTakenPrimary', StringType(), True),
    StructField('ActionTakenSecondary', StringType(), True),
    StructField('ActionTakenOther', StringType(), True),
    StructField('DetectorAlertedOccupants', StringType(), True),
    StructField('PropertyUse', StringType(), True),
    StructField('NumberOfSprinklerHeadsOperating', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('NeighborhoodDistrict', StringType(), True),
    StructField('Point', StringType(), True),
    StructField('DataAsOf', StringType(), True),
    StructField('DataLoadedAt', StringType(), True)
])

# Leer el archivo CSV usando DataFrameReader

sf_fire_file = "data/Fire_incidents.csv"
fire_df_csv = spark.read.csv(sf_fire_file, header=True)

selected_columns_df = fire_df_csv.select("Incident Number", "Exposure Number", "ID", "Address", "Incident Date",
                                     "Call Number", "Alarm DtTm", "Arrival DtTm", "Close DtTm", "City", "Zipcode",
                                     "Battalion", "Station Area", "Box", "Suppression Units", "Suppression Personnel",
                                     "EMS Units", "EMS Personnel", "Other Units", "Other Personnel", "First Unit On Scene",
                                     "Estimated Property Loss", "Estimated Contents Loss", "Fire Fatalities", "Fire Injuries",
                                     "Civilian Fatalities", "Civilian Injuries", "Number Of Alarms", "Primary Situation",
                                     "Mutual Aid", "Action Taken Primary", "Action Taken Secondary", "Action Taken Other",
                                     "Detector Alerted Occupants", "Property Use", "Number Of Sprinkler Heads Operating",
                                     "Supervisor District", "neighborhood_district", "Point", "Data_As_Of", "Data_Loaded_At")

# Mostrar algunas filas para verificar
fire_df = spark.createDataFrame(selected_columns_df.rdd, schema= fire_schema)

fire_df.show(5)

#parquet_path = "C:/Users/pablo.oller/PycharmProjects/pythonProject2/.venv/data/data_parquet"
#fire_df.write.mode("overwrite").format("parquet").save(parquet_path)


few_fire_df = (fire_df
 .select("IncidentNumber", "AlarmDtTm", "PrimarySituation")
 .where(~col("PrimarySituation").contains("Medical assist")))
few_fire_df.show(5, truncate=False)


(fire_df
.select("PrimarySituation")
.where(col("PrimarySituation").isNotNull())
.agg(countDistinct("PrimarySituation").alias("DistinctPrimarySituation"))
.show())

(fire_df
 .select("PrimarySituation")
 .where(col("PrimarySituation").isNotNull())
 .distinct()
 .show(10, False))


# Convertir AlarmDtTm y ArrivalDtTm a TimestampType con el formato correcto
fire_df = fire_df.withColumn("AlarmDtTm", to_timestamp("AlarmDtTm", "yyyy/MM/dd hh:mm:ss a")) \
                 .withColumn("ArrivalDtTm", to_timestamp("ArrivalDtTm", "yyyy/MM/dd hh:mm:ss a"))

# Calcular la columna Delay en minutos
fire_df = fire_df.withColumn("Delay", (col("ArrivalDtTm").cast("long") - col("AlarmDtTm").cast("long")) / 60)

# Mostrar algunas filas para verificar
fire_df.select("IncidentNumber", "AlarmDtTm", "ArrivalDtTm", "Delay").show(5, truncate=False)


fire_df = fire_df.withColumn("IncidentDate", to_date("IncidentDate", "yyyy/MM/dd"))
(fire_df.select(year('IncidentDate')).distinct().orderBy(year('IncidentDate')).show())



