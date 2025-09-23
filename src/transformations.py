# src/transformations.py

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, count, when, max as spark_max
from pyspark.sql.types import IntegerType, FloatType

def bronze_para_raw(spark: SparkSession):
    """
    BRONZE (RAW):
    - Lê os dados CSV brutos da "landing zone".
    - Converte os dados para o formato Delta Lake para otimizar as próximas leituras.
    - Esta é a primeira conversão de formato, criando a camada "Raw" dentro do Bronze.
    """
    logging.info("Iniciando a transformação de CSVs para Delta (Bronze Raw)...")
    
    landing_path = "data/bronze/landing/unzipped_csvs/"
    raw_path = "data/bronze/raw/"

    try:
        # Converte Empresas CSV para Delta
        df_empresas_raw_csv = spark.read.csv(f"{landing_path}*EMPRECSV", sep=';', inferSchema=False)
        df_empresas_raw_csv.write.format("delta").mode("overwrite").save(f"{raw_path}empresas")
        logging.info("Dados brutos de empresas convertidos para Delta na camada Raw.")
        
        # Converte Sócios CSV para Delta
        df_socios_raw_csv = spark.read.csv(f"{landing_path}*SOCIOCSV", sep=';', inferSchema=False)
        df_socios_raw_csv.write.format("delta").mode("overwrite").save(f"{raw_path}socios")
        logging.info("Dados brutos de sócios convertidos para Delta na camada Raw.")

    except Exception as e:
        logging.error(f"FALHA CRÍTICA ao converter CSV para Delta na camada Raw: {e}")
        raise e

def transformar_dados_silver(spark: SparkSession):
    """
    CAMADA SILVER:
    - Lê os dados brutos em formato Delta da camada Raw.
    - Valida se os dados de entrada não estão vazios.
    - Limpa, padroniza e converte os tipos de dados.
    - Salva o resultado como uma tabela Delta otimizada na camada Silver.
    """
    logging.info("Iniciando a transformação de dados para a Camada Silver...")

    raw_path = "data/bronze/raw/"
    silver_path = "data/silver/"

    # ----- Processamento de Empresas -----
    logging.info("Processando dados de empresas para Silver...")
    try:
        # Lê a tabela Delta de empresas da camada Raw
        df_empresas_raw = spark.read.format("delta").load(f"{raw_path}empresas")

        # VALIDAÇÃO DE DADOS (SANITY CHECK)
        if df_empresas_raw.isEmpty():
            raise ValueError("Dados de empresas não encontrados na camada Raw ou a tabela está vazia.")
        
        schema_empresas = {
            "_c0": "CNPJ_BASICO", "_c1": "RAZAO_SOCIAL_NOME_EMPRESARIAL", "_c2": "NATUREZA_JURIDICA",
            "_c3": "QUALIFICACAO_DO_RESPONSAVEL", "_c4": "CAPITAL_SOCIAL_DA_EMPRESA",
            "_c5": "PORTE_DA_EMPRESA", "_c6": "ENTE_FEDERATIVO_RESPONSAVEL"
        }
        for old_name, new_name in schema_empresas.items():
            df_empresas_raw = df_empresas_raw.withColumnRenamed(old_name, new_name)

        df_empresas_silver = df_empresas_raw.select(
            col("CNPJ_BASICO").alias("cnpj"),
            col("RAZAO_SOCIAL_NOME_EMPRESARIAL").alias("razao_social"),
            col("NATUREZA_JURIDICA").cast(IntegerType()).alias("natureza_juridica"),
            col("QUALIFICACAO_DO_RESPONSAVEL").cast(IntegerType()).alias("qualificacao_responsavel"),
            regexp_replace(col("CAPITAL_SOCIAL_DA_EMPRESA"), ",", ".").cast(FloatType()).alias("capital_social"),
            col("PORTE_DA_EMPRESA").alias("cod_porte")
        )
        
        # --- PONTO DE VERIFICAÇÃO ---
        logging.info("Amostra de dados de Empresas (Camada Silver):")
        df_empresas_silver.printSchema()
        df_empresas_silver.show(10, truncate=False)
        # ---------------------------

        df_empresas_silver.write.format("delta").mode("overwrite").save(f"{silver_path}empresas")
        logging.info("Dados de empresas salvos na camada Silver em formato Delta.")
    except Exception as e:
        logging.error(f"FALHA CRÍTICA ao processar dados de empresas para Silver: {e}")
        raise e

    # ----- Processamento de Sócios -----
    logging.info("Processando dados de sócios para Silver...")
    try:
        df_socios_raw = spark.read.format("delta").load(f"{raw_path}socios")
        
        if df_socios_raw.isEmpty():
            raise ValueError("Dados de sócios não encontrados na camada Raw ou a tabela está vazia.")

        schema_socios = {
            "_c0": "CNPJ_BASICO", "_c1": "IDENTIFICADOR_DE_SOCIO", "_c2": "NOME_DO_SOCIO_OU_RAZAO_SOCIAL",
            "_c3": "CNPJ_CPF_DO_SOCIO", "_c4": "QUALIFICACAO_DO_SOCIO"
        }
        for old_name, new_name in schema_socios.items():
            df_socios_raw = df_socios_raw.withColumnRenamed(old_name, new_name)
        
        df_socios_silver = df_socios_raw.select(
            col("CNPJ_BASICO").alias("cnpj"),
            col("IDENTIFICADOR_DE_SOCIO").cast(IntegerType()).alias("tipo_socio"),
            col("NOME_DO_SOCIO_OU_RAZAO_SOCIAL").alias("nome_socio"),
            col("CNPJ_CPF_DO_SOCIO").alias("documento_socio"),
            col("QUALIFICACAO_DO_SOCIO").alias("codigo_qualificacao_socio")
        )
        
        # --- PONTO DE VERIFICAÇÃO ---
        logging.info("Amostra de dados de Sócios (Camada Silver):")
        df_socios_silver.printSchema()
        df_socios_silver.show(10, truncate=False)
        # ---------------------------
        
        df_socios_silver.write.format("delta").mode("overwrite").save(f"{silver_path}socios")
        logging.info("Dados de sócios salvos na camada Silver em formato Delta.")
    except Exception as e:
        logging.error(f"FALHA CRÍTICA ao processar dados de sócios para Silver: {e}")
        raise e

    logging.info("Transformação para a Camada Silver concluída com sucesso.")

def transformar_dados_gold(spark: SparkSession):
    """
    CAMADA GOLD:
    - Lê as tabelas Delta limpas da camada Silver.
    - Aplica as regras de negócio e agregações.
    - Salva o resultado final como uma tabela Delta na camada Gold.
    - Retorna o DataFrame final para carregamento no banco de dados.
    """
    logging.info("Iniciando a transformação de dados para a Camada Gold...")
    silver_path = "data/silver/"
    gold_path = "data/gold/"

    # Carrega as tabelas limpas da camada Silver
    df_empresas = spark.read.format("delta").load(f"{silver_path}empresas")
    df_socios = spark.read.format("delta").load(f"{silver_path}socios")

    # REGRA 1: Calcular a quantidade de sócios e se há sócio estrangeiro
    df_socios_agg = df_socios.groupBy("cnpj").agg(
        # 1.1: Conta o número de sócios para cada CNPJ
        count("nome_socio").alias("qtde_socios"),
        # 1.2: Cria a flag 'flag_socio_estrangeiro'. O tipo 3 identifica sócio estrangeiro.
        # A função max() garante que se houver um único 'True' no grupo, o resultado será 'True'.
        spark_max(when(col("tipo_socio") == 3, True).otherwise(False)).alias("flag_socio_estrangeiro")
    )

    # Junta os dados de empresas com os dados agregados de sócios
    df_gold = df_empresas.join(df_socios_agg, "cnpj", "left")
    
    # Preenche valores nulos para empresas que não têm sócios
    df_gold = df_gold.fillna(0, subset=["qtde_socios"])
    df_gold = df_gold.fillna(False, subset=["flag_socio_estrangeiro"])

    # REGRA 2: Calcular a coluna 'doc_alvo'
    # A regra é: True se o porte da empresa for '03' (PEQUENO PORTE) E a quantidade de sócios for maior que 1.
    df_gold = df_gold.withColumn("doc_alvo",
        when((col("cod_porte") == "03") & (col("qtde_socios") > 1), True)
        .otherwise(False)
    )

    # Seleciona as colunas finais para o output do desafio
    df_gold_final = df_gold.select(
        "cnpj",
        "qtde_socios",
        "flag_socio_estrangeiro",
        "doc_alvo"
    )

    # --- PONTO DE VERIFICAÇÃO ---
    logging.info("Amostra de dados Finais (Camada Gold):")
    df_gold_final.printSchema()
    df_gold_final.show(10, truncate=False)
    # ---------------------------

    # Salva o resultado em formato Delta na camada Gold para auditoria e reuso
    df_gold_final.write.format("delta").mode("overwrite").save(f"{gold_path}resultado_final")
    logging.info("Dados da camada Gold salvos em formato Delta.")
    
    logging.info("Transformação para a Camada Gold concluída.")
    
    # Retorna o DataFrame para a etapa de carregamento no banco de dados
    return df_gold_final