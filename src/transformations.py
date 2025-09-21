# src/transformations.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import IntegerType, FloatType


def transformar_dados_silver(spark: SparkSession):
    """
    Função principal para orquestrar a transformação dos dados da camada Bronze para a Silver.
    Lê os arquivos brutos, aplica o schema, limpa e salva em formato Parquet.
    """
    print("Iniciando a transformação de dados para a Camada Silver...")

    # Define os caminhos de entrada (bronze) e saída (silver)
    bronze_path = "data/bronze/"
    silver_path = "data/silver/"

    # ----- Processamento de Empresas -----
    print("Processando dados de empresas...")
    try:
        # Os arquivos da Receita são CSV sem cabeçalho e com o separador ';'
        # Usamos o * para ler todos os arquivos de empresa que foram descompactados
        df_empresas = spark.read.csv(f"{bronze_path}*EMPRECSV", sep=';', inferSchema=False)

        # Renomeia as colunas com base no layout fornecido pela Receita/desafio
        df_empresas = df_empresas.withColumnRenamed("_c0", "cnpj_basico") \
                                 .withColumnRenamed("_c1", "razao_social") \
                                 .withColumnRenamed("_c2", "natureza_juridica") \
                                 .withColumnRenamed("_c3", "qualificacao_responsavel") \
                                 .withColumnRenamed("_c4", "capital_social") \
                                 .withColumnRenamed("_c5", "porte_empresa") \
                                 .withColumnRenamed("_c6", "ente_federativo_responsavel")

        # Seleciona apenas as colunas que precisamos para o desafio
        df_empresas_silver = df_empresas.select(
            col("cnpj_basico").alias("cnpj"),
            "razao_social",
            col("natureza_juridica").cast(IntegerType()),
            col("qualificacao_responsavel").cast(IntegerType()),
            regexp_replace(col("capital_social"), ",", ".").cast(FloatType()).alias("capital_social"),
            col("porte_empresa").alias("cod_porte")
        )

        df_empresas_silver.write.mode("overwrite").parquet(f"{silver_path}empresas")
        print("Dados de empresas salvos na camada Silver.")
    except Exception as e:
        print(f"Erro ao processar dados de empresas: {e}")


    # ----- Processamento de Sócios -----
    print("Processando dados de sócios...")
    try:
        df_socios = spark.read.csv(f"{bronze_path}*SOCIOCSV", sep=';', inferSchema=False)

        df_socios = df_socios.withColumnRenamed("_c0", "cnpj_basico") \
                             .withColumnRenamed("_c1", "identificador_socio") \
                             .withColumnRenamed("_c2", "nome_socio") \
                             .withColumnRenamed("_c3", "documento_socio") \
                             .withColumnRenamed("_c4", "codigo_qualificacao_socio")

        df_socios_silver = df_socios.select(
            col("cnpj_basico").alias("cnpj"),
            col("identificador_socio").cast(IntegerType()).alias("tipo_socio"),
            "nome_socio",
            "documento_socio",
            "codigo_qualificacao_socio"
        )
        
        df_socios_silver.write.mode("overwrite").parquet(f"{silver_path}socios")
        print("Dados de sócios salvos na camada Silver.")
    except Exception as e:
        print(f"Erro ao processar dados de sócios: {e}")

    print("Transformação para a Camada Silver concluída.")


# Cria Função da Camada Gold
from pyspark.sql.functions import count, when, max as spark_max

def transformar_dados_gold(spark: SparkSession):
    """
    Lê os dados da camada Silver, aplica as regras de negócio para calcular
    a quantidade de sócios, flag de sócio estrangeiro e o documento alvo,
    e retorna o DataFrame final da camada Gold.
    """
    print("Iniciando a transformação de dados para a Camada Gold...")
    silver_path = "data/silver/"

    # 1. Ler os dados da camada Silver
    df_empresas = spark.read.parquet(f"{silver_path}empresas")
    df_socios = spark.read.parquet(f"{silver_path}socios")

    # 2. Calcular qtde_socios e flag_socio_estrangeiro
    #    Agrupamos por CNPJ para fazer os cálculos.
    df_socios_agg = df_socios.groupBy("cnpj").agg(
        count("nome_socio").alias("qtde_socios"),
        # O identificador de sócio estrangeiro é 3. Criamos uma flag se encontrarmos algum.
        spark_max(when(col("tipo_socio") == 3, True).otherwise(False)).alias("flag_socio_estrangeiro")
    )

    # 3. Juntar os dados de empresas com os dados agregados de sócios
    #    Usamos um left join para manter todas as empresas, mesmo as que não têm sócios listados.
    df_gold = df_empresas.join(df_socios_agg, "cnpj", "left")
    
    # Preenche com 0 e False os CNPJs que não tinham sócios
    df_gold = df_gold.fillna(0, subset=["qtde_socios"])
    df_gold = df_gold.fillna(False, subset=["flag_socio_estrangeiro"])

    # 4. Calcular a coluna doc_alvo
    #    A regra é: porte da empresa = 03 E qtde_socios > 1
    df_gold = df_gold.withColumn("doc_alvo",
        when((col("cod_porte") == "03") & (col("qtde_socios") > 1), True)
        .otherwise(False)
    )

    # 5. Selecionar as colunas finais para o output
    df_gold_final = df_gold.select(
        "cnpj",
        "qtde_socios",
        "flag_socio_estrangeiro",
        "doc_alvo"
    )

    print("Transformação para a Camada Gold concluída.")
    
    # Retornamos o DataFrame final para ser carregado no banco de dados
    return df_gold_final