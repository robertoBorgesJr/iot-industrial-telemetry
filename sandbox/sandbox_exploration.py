from pyspark.sql import SparkSession

# Inicializa a sessão do Spark (que cria o objeto 'spark')
spark = SparkSession.builder.appName("VerificarVersao").getOrCreate()

# Executa o seu comando
print(spark.version)