
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, count
from pyspark.sql import functions as F
from lib.config import (
    get_postgres_config,
    get_sqlserverprotheus_config,
)
import time


# =========================================================
# FINALIZAR SESSÃO ANTERIOR
# =========================================================

try:
    spark.stop()
except:
    pass


# =========================================================
# CONFIGURAÇÕES DE CONEXÃO
# =========================================================

POSTGRES_CONFIG = get_postgres_config()
SQLSERVER_CONFIG = get_sqlserverprotheus_config()

# ---------------------------------------------------------
# PostgreSQL destino
# ---------------------------------------------------------

POSTGRES_HOST = POSTGRES_CONFIG["host"]
POSTGRES_PORT = POSTGRES_CONFIG["port"]
TARGET_POSTGRES_DATABASE = POSTGRES_CONFIG["database"]

POSTGRES_USER = POSTGRES_CONFIG["user"]
POSTGRES_PASSWORD = POSTGRES_CONFIG["password"]

# ---------------------------------------------------------
# SQL Server Protheus origem
# ---------------------------------------------------------

SQL_SERVER_HOST = SQLSERVER_CONFIG["host"]
SQL_SERVER_PORT = SQLSERVER_CONFIG["port"]
SQL_SERVER_DATABASE = SQLSERVER_CONFIG["database"]

SQL_SERVER_USER = SQLSERVER_CONFIG["user"]
SQL_SERVER_PASSWORD = SQLSERVER_CONFIG["password"]


# =========================================================
# CONFIGURAÇÕES DO JOB
# =========================================================

TARGET_TABLE = "raw.SFT"

NUM_PARTITIONS = 12


# =========================================================
# QUERY PRINCIPAL
# =========================================================

inicio_execucao = time.time()

QUERY = """
    SELECT
        a.*
        , CURRENT_TIMESTAMP as "DATA_CARGA"
    FROM (
    SELECT 
        'PROTHEUS' as origem
        , a.FT_FILIAL
        , a.FT_NFISCAL
        , a.FT_SERIE
        , a.FT_PRODUTO
        , a.FT_LOJA
        , a.FT_ITEM
        , a.FT_CLIEFOR
        , a.FT_CFOP
        , a.FT_TIPOMOV
        , a.FT_ESTOQUE
        , a.FT_EMISSAO
        , a.FT_ENTRADA
        , a.FT_DTCANC
        , a.FT_PDV
        , a.FT_TOTAL
        , a.FT_DESCONT
        , a.FT_QUANT
        , a.FT_TES
        , a.R_E_C_N_O_
        , a.D_E_L_E_T_
        , a.S_T_A_M_P_
    FROM SFT010 a WITH(NOLOCK)
    WHERE a.D_E_L_E_T_ <> '*'
    AND CAST (a.FT_EMISSAO AS DATE) >= '2025-01-01'

    UNION ALL

    SELECT  
        'TRR-PROTHEUS' AS origem
        , a.FT_FILIAL
        , a.FT_NFISCAL
        , a.FT_SERIE
        , a.FT_PRODUTO
        , a.FT_LOJA
        , a.FT_ITEM
        , a.FT_CLIEFOR
        , a.FT_CFOP
        , a.FT_TIPOMOV
        , a.FT_ESTOQUE
        , a.FT_EMISSAO
        , a.FT_ENTRADA
        , a.FT_DTCANC
        , a.FT_PDV
        , a.FT_TOTAL
        , a.FT_DESCONT
        , a.FT_QUANT
        , a.FT_TES
        , a.R_E_C_N_O_
        , a.D_E_L_E_T_
        , a.S_T_A_M_P_
    FROM SFT020 a WITH(NOLOCK)
    WHERE a.D_E_L_E_T_ <> '*'
    AND CAST (a.FT_EMISSAO AS DATE) >= '2025-01-01'

    UNION ALL 

    SELECT
        'TRR-DIST-PROTHEUS' as origem
        , a.FT_FILIAL
        , a.FT_NFISCAL
        , a.FT_SERIE
        , a.FT_PRODUTO
        , a.FT_LOJA
        , a.FT_ITEM
        , a.FT_CLIEFOR
        , a.FT_CFOP
        , a.FT_TIPOMOV
        , a.FT_ESTOQUE
        , a.FT_EMISSAO
        , a.FT_ENTRADA
        , a.FT_DTCANC
        , a.FT_PDV
        , a.FT_TOTAL
        , a.FT_DESCONT
        , a.FT_QUANT
        , a.FT_TES
        , a.R_E_C_N_O_
        , a.D_E_L_E_T_
        , a.S_T_A_M_P_
    FROM SFT030 a WITH(NOLOCK)
    WHERE a.D_E_L_E_T_ <> '*'
    AND CAST (a.FT_EMISSAO AS DATE) >= '2025-01-01'

    UNION ALL

    SELECT 
        'URB-PROTHEUS' as origem
        , a.FT_FILIAL
        , a.FT_NFISCAL
        , a.FT_SERIE
        , a.FT_PRODUTO
        , a.FT_LOJA
        , a.FT_ITEM
        , a.FT_CLIEFOR
        , a.FT_CFOP
        , a.FT_TIPOMOV
        , a.FT_ESTOQUE
        , a.FT_EMISSAO
        , a.FT_ENTRADA
        , a.FT_DTCANC
        , a.FT_PDV
        , a.FT_TOTAL
        , a.FT_DESCONT
        , a.FT_QUANT
        , a.FT_TES
        , a.R_E_C_N_O_
        , a.D_E_L_E_T_
        , a.S_T_A_M_P_
    FROM SFT090 a WITH(NOLOCK)
    WHERE a.D_E_L_E_T_ <> '*'
    AND CAST (a.FT_EMISSAO AS DATE) >= '2025-01-01'
    ) a
"""


# =========================================================
# QUERY MIN/MAX
# =========================================================

QUERY_MIN_MAX = """
SELECT
    MIN(a."R_E_C_N_O_") AS menor,
    MAX(a."R_E_C_N_O_") AS maior
FROM (
    SELECT 
        'PROTHEUS' as origem
        , a.FT_FILIAL
        , a.FT_NFISCAL
        , a.FT_SERIE
        , a.FT_PRODUTO
        , a.FT_LOJA
        , a.FT_ITEM
        , a.FT_CLIEFOR
        , a.FT_CFOP
        , a.FT_TIPOMOV
        , a.FT_ESTOQUE
        , a.FT_EMISSAO
        , a.FT_ENTRADA
        , a.FT_DTCANC
        , a.FT_PDV
        , a.FT_TOTAL
        , a.FT_DESCONT
        , a.FT_QUANT
        , a.FT_TES
        , a.R_E_C_N_O_
        , a.D_E_L_E_T_
        , a.S_T_A_M_P_
    FROM SFT010 a WITH(NOLOCK)
    WHERE a.D_E_L_E_T_ <> '*'
    AND CAST (a.FT_EMISSAO AS DATE) >= '2025-01-01'

    UNION ALL

    SELECT  
        'TRR-PROTHEUS' AS origem
        , a.FT_FILIAL
        , a.FT_NFISCAL
        , a.FT_SERIE
        , a.FT_PRODUTO
        , a.FT_LOJA
        , a.FT_ITEM
        , a.FT_CLIEFOR
        , a.FT_CFOP
        , a.FT_TIPOMOV
        , a.FT_ESTOQUE
        , a.FT_EMISSAO
        , a.FT_ENTRADA
        , a.FT_DTCANC
        , a.FT_PDV
        , a.FT_TOTAL
        , a.FT_DESCONT
        , a.FT_QUANT
        , a.FT_TES
        , a.R_E_C_N_O_
        , a.D_E_L_E_T_
        , a.S_T_A_M_P_
    FROM SFT020 a WITH(NOLOCK)
    WHERE a.D_E_L_E_T_ <> '*'
    AND CAST (a.FT_EMISSAO AS DATE) >= '2025-01-01'

    UNION ALL 

    SELECT
        'TRR-DIST-PROTHEUS' as origem
        , a.FT_FILIAL
        , a.FT_NFISCAL
        , a.FT_SERIE
        , a.FT_PRODUTO
        , a.FT_LOJA
        , a.FT_ITEM
        , a.FT_CLIEFOR
        , a.FT_CFOP
        , a.FT_TIPOMOV
        , a.FT_ESTOQUE
        , a.FT_EMISSAO
        , a.FT_ENTRADA
        , a.FT_DTCANC
        , a.FT_PDV
        , a.FT_TOTAL
        , a.FT_DESCONT
        , a.FT_QUANT
        , a.FT_TES
        , a.R_E_C_N_O_
        , a.D_E_L_E_T_
        , a.S_T_A_M_P_
    FROM SFT030 a WITH(NOLOCK)
    WHERE a.D_E_L_E_T_ <> '*'
    AND CAST (a.FT_EMISSAO AS DATE) >= '2025-01-01'

    UNION ALL

    SELECT 
        'URB-PROTHEUS' as origem
        , a.FT_FILIAL
        , a.FT_NFISCAL
        , a.FT_SERIE
        , a.FT_PRODUTO
        , a.FT_LOJA
        , a.FT_ITEM
        , a.FT_CLIEFOR
        , a.FT_CFOP
        , a.FT_TIPOMOV
        , a.FT_ESTOQUE
        , a.FT_EMISSAO
        , a.FT_ENTRADA
        , a.FT_DTCANC
        , a.FT_PDV
        , a.FT_TOTAL
        , a.FT_DESCONT
        , a.FT_QUANT
        , a.FT_TES
        , a.R_E_C_N_O_
        , a.D_E_L_E_T_
        , a.S_T_A_M_P_
    FROM SFT090 a WITH(NOLOCK)
    WHERE a.D_E_L_E_T_ <> '*'
    AND CAST (a.FT_EMISSAO AS DATE) >= '2025-01-01'
) a
"""


# =========================================================
# SPARK SESSION
# =========================================================

print("#########################################################")
print("Criando sessão Spark...")

inicio = time.time()

spark = (
    SparkSession.builder
    .appName(TARGET_TABLE)
    .master("spark://192.168.100.104:7077")
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
    .config("spark.default.parallelism", "12")
    .config("spark.sql.shuffle.partitions", "12")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config(
        "spark.serializer",
        "org.apache.spark.serializer.KryoSerializer"
    )
    .getOrCreate()
)

print(f"Spark iniciado em {time.time() - inicio:.2f}s")
print("#########################################################")


# =========================================================
# JDBC URLS
# =========================================================

SQLSERVER_URL = (
    f"jdbc:sqlserver://{SQL_SERVER_HOST}:{SQL_SERVER_PORT};"
    f"databaseName={SQL_SERVER_DATABASE};"
    "encrypt=true;"
    "trustServerCertificate=true"
)

TARGET_POSTGRES_URL = (
    f"jdbc:postgresql://"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/"
    f"{TARGET_POSTGRES_DATABASE}"
)

POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}


# =========================================================
# BUSCAR MIN E MAX
# =========================================================

print("#########################################################")
print("Buscando limites para particionamento...")

inicio = time.time()

min_max = (
    spark.read
    .format("jdbc")
    .option("url", SQLSERVER_URL)
    .option("query", QUERY_MIN_MAX)
    .option("user", SQL_SERVER_USER)
    .option("password", SQL_SERVER_PASSWORD)
    .option(
        "driver",
        "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    )
    .load()
    .first()
)

menor = int(min_max.menor)
maior = int(min_max.maior)

print(f"Menor RECNO : {menor}")
print(f"Maior RECNO : {maior}")

print(f"Tempo: {time.time() - inicio:.2f}s")
print("#########################################################")


# =========================================================
# LEITURA
# =========================================================

print("#########################################################")
print("Lendo dados da origem...")

inicio = time.time()

df = (
    spark.read
    .format("jdbc")
    .option("url", SQLSERVER_URL)
    .option("dbtable", f"({QUERY}) origem")
    .option("user", SQL_SERVER_USER)
    .option("password", SQL_SERVER_PASSWORD)
    .option(
        "driver",
        "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    )
    .option("partitionColumn", "R_E_C_N_O_")
    .option("lowerBound", menor)
    .option("upperBound", maior)
    .option("numPartitions", NUM_PARTITIONS)
    .option("fetchsize", 5000)
    .load()
)

print(f"Tempo leitura: {time.time() - inicio:.2f}s")
print("#########################################################")


# =========================================================
# REPARTITION
# =========================================================

print("#########################################################")
print("Reparticionando DataFrame...")

inicio = time.time()

#df = df.repartition(12)

print("Partições após JDBC:", df.rdd.getNumPartitions())


print(
    f"Tempo repartition: "
    f"{time.time() - inicio:.2f}s"
)

print("#########################################################")


# =========================================================
# ESCRITA
# =========================================================

print("#########################################################")
print("Escrevendo no PostgreSQL destino...")

inicio = time.time()

(
    df.write
    .format("jdbc")
    .option("url", TARGET_POSTGRES_URL)
    .option("dbtable", TARGET_TABLE)
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .option("batchsize", 20000)
    .mode("overwrite")
    .save()
)

print(f"Tempo escrita: {time.time() - inicio:.2f}s")
print("#########################################################")


# =========================================================
# FINALIZAÇÃO
# =========================================================

fim_execucao = time.time() - inicio_execucao

spark.stop()

print("#########################################################")
print(
    f"Spark finalizado com sucesso. "
    f"{fim_execucao / 60:.2f} minutos"
)
print("#########################################################")
