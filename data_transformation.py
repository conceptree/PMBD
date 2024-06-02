import os

# Definir a mesma versão do Python para driver e worker
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.11'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.11'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# ----------------------------------------------------------------------------------------------------------------- #

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, floor

# Inicializar a Spark Session
spark = SparkSession.builder \
    .appName("Weather Dataset with Location") \
    .getOrCreate()

# Carregar o dataset sample
file_path = "./assets/sample_dataset.csv"
weather_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Carregar o dataset das estações
stations_file_path = "./assets/ghcnd_stations.csv"
stations_df = spark.read.csv(stations_file_path, header=True, inferSchema=True)

# Para evitar conflito com as colunas que já existem no dataset original
stations_df = stations_df.withColumnRenamed("elevation", "station_elevation")  \
                        .withColumnRenamed("latitude", "station_latitude") \
                        .withColumnRenamed("longitude", "station_longitude")

# Juntar o DataFrame original com as localizações das estações
weather_df_with_location = weather_df.join(stations_df, weather_df.ID == stations_df.station_id, how='left')

# Transformar as temperaturas de décimos de grau para graus Celsius
weather_df_with_location = weather_df_with_location \
    .withColumn("maxTempCelsius", col("TMAX") / 10) \
    .withColumn("minTempCelsius", col("TMIN") / 10)

# Arredondar a elevação para o número inteiro mais próximo
weather_df_with_location = weather_df_with_location.withColumn("elevationRounded", floor(round(col("Elevation"))))

# Renomear colunas EVAP e PRCP
weather_df_with_location = weather_df_with_location.withColumnRenamed("EVAP", "Evaporation") \
                                                   .withColumnRenamed("PRCP", "Precipitation")

# Selecionar colunas relevantes
weather_df_with_location = weather_df_with_location.select(
    "ID", "DATE", "maxTempCelsius", "minTempCelsius", "Evaporation", "Precipitation",
    "Latitude", "Longitude", "elevationRounded", "name"
)

# Salvar resultado final
output_path = "./assets/sample_weather_dataset_with_location.csv"
weather_df_with_location.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

print(f"Dataset final salvo em {output_path}")

# Encerrar a Spark Session
spark.stop()


