from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


spark = (
    SparkSession.builder
    .appName("Teste Airflow Spark")
    .getOrCreate()
)


print("==========================================")
print("        INÍCIO DO JOB PYSPARK")
print("==========================================")

print(f"Spark Version: {spark.version}")


dados = [
    (1, "Produto A", 10, 100.00),
    (2, "Produto B", 5, 200.00),
    (3, "Produto A", 7, 100.00),
    (4, "Produto C", 3, 500.00),
    (5, "Produto B", 8, 200.00),
]


df = spark.createDataFrame(
    dados,
    ["id", "produto", "quantidade", "valor_unitario"],
)


print("==========================================")
print("        DADOS ORIGINAIS")
print("==========================================")

df.show()


df_resultado = (
    df
    .withColumn(
        "valor_total",
        col("quantidade") * col("valor_unitario")
    )
    .groupBy("produto")
    .agg(
        sum("quantidade").alias("quantidade_total"),
        sum("valor_total").alias("valor_total"),
    )
    .orderBy("produto")
)


print("==========================================")
print("        RESULTADO FINAL")
print("==========================================")

df_resultado.show()


print("==========================================")
print("        JOB FINALIZADO COM SUCESSO")
print("==========================================")


spark.stop()
