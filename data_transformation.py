import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, floor, mean as _mean, stddev as _stddev

# Definir a mesma versão do Python para driver e worker
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.11'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.11'

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
                        .withColumnRenamed("name", "Location") \
                        .withColumnRenamed("longitude", "station_longitude")

# Juntar o DataFrame original com as localizações das estações
weather_df_with_location = weather_df.join(stations_df, weather_df.ID == stations_df.station_id, how='left')

# Transformar as temperaturas de décimos de grau para graus Celsius
weather_df_with_location = weather_df_with_location \
    .withColumn("temp_max", col("TMAX") / 10) \
    .withColumn("temp_min", col("TMIN") / 10)

# Método IQR para detectar outliers
def calcular_limites_iqr(df, coluna):
    quantiles = df.approxQuantile(coluna, [0.25, 0.75], 0.0)
    q1, q3 = quantiles
    iqr = q3 - q1
    limite_inferior = q1 - 1.5 * iqr
    limite_superior = q3 + 1.5 * iqr
    return limite_inferior, limite_superior

temp_max_inferior, temp_max_superior = calcular_limites_iqr(weather_df_with_location, "temp_max")
temp_min_inferior, temp_min_superior = calcular_limites_iqr(weather_df_with_location, "temp_min")

weather_df_with_location = weather_df_with_location.filter(
    (col("temp_max") >= temp_max_inferior) & (col("temp_max") <= temp_max_superior) &
    (col("temp_min") >= temp_min_inferior) & (col("temp_min") <= temp_min_superior)
)

# Método Z-score para detectar outliers
def calcular_limites_z_score(df, coluna):
    stats = df.select(_mean(col(coluna)).alias('mean'), _stddev(col(coluna)).alias('stddev')).collect()
    mean = stats[0]['mean']
    stddev = stats[0]['stddev']
    limite_inferior = mean - 3 * stddev
    limite_superior = mean + 3 * stddev
    return limite_inferior, limite_superior

temp_max_inferior, temp_max_superior = calcular_limites_z_score(weather_df_with_location, "temp_max")
temp_min_inferior, temp_min_superior = calcular_limites_z_score(weather_df_with_location, "temp_min")

weather_df_with_location = weather_df_with_location.filter(
    (col("temp_max") >= temp_max_inferior) & (col("temp_max") <= temp_max_superior) &
    (col("temp_min") >= temp_min_inferior) & (col("temp_min") <= temp_min_superior)
)

# Arredondar a elevação para o número inteiro mais próximo
weather_df_with_location = weather_df_with_location.withColumn("Elevation", floor(round(col("Elevation"))))

# Renomear colunas EVAP e PRCP
weather_df_with_location = weather_df_with_location.withColumnRenamed("EVAP", "Evaporation") \
                                                   .withColumnRenamed("PRCP", "Precipitation")

# Filtrar entradas com null em Precipitation, temp_max ou temp_min
weather_df_with_location = weather_df_with_location.filter(
    col("Precipitation").isNotNull() &
    col("temp_max").isNotNull() &
    col("temp_min").isNotNull()
)

# Selecionar colunas relevantes
weather_df_with_location = weather_df_with_location.select(
    "ID", "DATE", "temp_max", "temp_min", "Precipitation",
    "Latitude", "Longitude", "Elevation", "Location"
)

# Salvar resultado final
output_path = "./assets/sample_weather_dataset_with_location.csv"
weather_df_with_location.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

print(f"Dataset final salvo em {output_path}")

# Encerrar a Spark Session
spark.stop()
