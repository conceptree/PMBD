from pyspark.sql import SparkSession

# Inicializar a Spark Session
spark = SparkSession.builder \
    .appName("Count Entries in Dataset") \
    .getOrCreate()

# Carregar o dataset completo
file_path = "./assets/us_weather_dataset.csv"
weather_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Contar o número de entradas únicas de coordenadas (estação meteorológica)
unique_stations = weather_df.select('latitude', 'longitude').distinct().count()

print(f"Número de estações meteorológicas únicas: {unique_stations}")

# Encerrar a Spark Session
spark.stop()
