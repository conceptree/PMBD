import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark import StorageLevel

# Definir a mesma versão do Python para driver e worker
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.11'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.11'

spark = SparkSession.builder \
    .appName("Weather Dataset with Random Forest") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

file_path = "./assets/sample_dataset.csv"
weather_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Verificar as colunas
weather_df.printSchema()

# Persistir em disco
weather_df.persist(StorageLevel.DISK_ONLY)

# Preparar os dados para o modelo
feature_columns = ["Latitude", "Longitude", "Elevation"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Transformar o DataFrame
weather_df = assembler.transform(weather_df)

# Dividir o conjunto de dados em treino e teste
train_df, test_df = weather_df.randomSplit([0.8, 0.2], seed=42)

# Treinar e avaliar o modelo
def treinar_e_avaliar_random_forest(label_col, train_df, test_df):
    # Treinar o modelo de Random Forest
    rf = RandomForestRegressor(featuresCol="features", labelCol=label_col, numTrees=100)
    rf_model = rf.fit(train_df)

    # Fazer previsões no conjunto de teste
    predictions = rf_model.transform(test_df)

    # Avaliar o modelo
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE) no conjunto de teste para {label_col}: {rmse}")

    # Converter para Pandas para visualização
    predictions_pd = predictions.select("prediction", label_col).toPandas()

    # Gerar gráfico de dispersão
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x=label_col, y="prediction", data=predictions_pd)
    plt.xlabel(f"Actual {label_col}")
    plt.ylabel(f"Predicted {label_col}")
    plt.title(f"Actual vs Predicted {label_col}")
    plt.show()

    return rf_model, predictions

# Iniciar Treino

# temp_max
rf_model_temp_max, predictions_temp_max = treinar_e_avaliar_random_forest("temp_max", train_df, test_df)

# temp_min
rf_model_temp_min, predictions_temp_min = treinar_e_avaliar_random_forest("temp_min", train_df, test_df)

# Precipitation
rf_model_precipitation, predictions_precipitation = treinar_e_avaliar_random_forest("Precipitation", train_df, test_df)

# Liberar recursos de memória
weather_df.unpersist()

# Encerrar a Spark Session
spark.stop()
