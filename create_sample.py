import os
from pyspark.sql import SparkSession

# Inicializar a Spark Session
spark = SparkSession.builder \
    .appName("Weather Dataset Sample") \
    .getOrCreate()

# Definir o caminho para o arquivo
file_path = "BD_Project/assets/us_weather_dataset.csv"

# Verificar se o arquivo existe
if not os.path.exists(file_path):
    raise FileNotFoundError(f"O arquivo não foi encontrado: {file_path}")

# Carregar o dataset completo
weather_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Criar um sample de 1% dos dados
sampled_df = weather_df.sample(fraction=0.01, seed=42)

# Salvar o sample em um novo arquivo CSV
output_path = "BD_Project/assets/sample_dataset"
sampled_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

print(f"Sample criado e salvo em {output_path}")

# Encerrar a Spark Session
spark.stop()
