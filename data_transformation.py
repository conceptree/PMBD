from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from geolocation_threading import geocode_locations

# Inicializar a Spark Session
spark = SparkSession.builder \
    .appName("Weather Dataset with Location") \
    .getOrCreate()

# Carregar o dataset sample
file_path = "./assets/sample_dataset.csv"
weather_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Extração de coordenadas únicas
lat_lon_df = weather_df.select('latitude', 'longitude').distinct()
lat_lon_list = [(row['latitude'], row['longitude']) for row in lat_lon_df.collect()]

# Geocodificar locais
result_list = geocode_locations(lat_lon_list)

# Criar DataFrame com resultados
lat_lon_and_location = [(lat_lon_list[i][0], lat_lon_list[i][1], result_list[i]) for i in range(len(lat_lon_list))]
lat_lon_spark_df = spark.createDataFrame(lat_lon_and_location, schema=['latitude', 'longitude', 'location'])

# Juntar o DataFrame original com as localizações
weather_df_with_location = weather_df.join(lat_lon_spark_df, on=['latitude', 'longitude'], how='left')

# Transformar as temperaturas de décimos de grau para graus Celsius
weather_df_with_location = weather_df_with_location \
    .withColumn("maxTempCelsius", col("TMAX") / 10) \
    .withColumn("minTempCelsius", col("TMIN") / 10)

# Arredondar a elevação para o número inteiro mais próximo
weather_df_with_location = weather_df_with_location.withColumn("elevationRounded", round(col("Elevation")))

# Salvar resultado final
output_path = "./assets/sample_weather_dataset_with_location.csv"
weather_df_with_location.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

print(f"Dataset final salvo em {output_path}")

# Encerrar a Spark Session
spark.stop()
