# src/transformations.py

import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import IntegerType, FloatType

# Definição dos caminhos
RAW_PATH = Path("data/bronze/raw")
SILVER_PATH = Path("data/silver")

def executar_transformacao_silver(spark: SparkSession):
    """
    CAMADA SILVER:
    Lê os dados da camada Bronze (raw), aplica limpezas, renomeia colunas
    e converte tipos de dados, salvando o resultado na camada Silver.
    """
    logging.info("--- Iniciando criação da Camada Silver ---")
    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    try:
        # 1.----- Processamento de Empresas -----
        logging.info("Processando empresas: Raw -> Silver...")
        df_empresas_raw = spark.read.format("delta").load(str(RAW_PATH / "empresas"))

        # Valida se a tabela de entrada não está vazia antes de prosseguir
        if df_empresas_raw.isEmpty():
            raise ValueError("A tabela de empresas na camada Raw está vazia.")
        
        # O schema é definido com base na documentação da Receita Federal
        df_empresas_silver = df_empresas_raw.select(
            col("_c0").alias("cnpj"),
            col("_c1").alias("razao_social"),
            col("_c2").cast(IntegerType()).alias("natureza_juridica"),
            col("_c3").cast(IntegerType()).alias("qualificacao_responsavel"),
            regexp_replace(col("_c4"), ",", ".").cast(FloatType()).alias("capital_social"),
            col("_c5").alias("cod_porte")
        )
        
        # --- PONTO DE VERIFICAÇÃO ---
        logging.info("Amostra de dados de Empresas (Camada Silver):")
        df_empresas_silver.printSchema()
        df_empresas_silver.show(5, truncate=False)
        # ---------------------------

        df_empresas_silver.write.format("delta").mode("overwrite").save(str(SILVER_PATH / "empresas"))

        # 2. ----- Processamento de Sócios -----
        logging.info("Processando sócios: Raw -> Silver...")
        df_socios_raw = spark.read.format("delta").load(str(RAW_PATH / "socios"))
        
        # Valida se a tabela de entrada não está vazia antes de prosseguir
        if df_socios_raw.isEmpty():
            raise ValueError("A tabela de sócios na camada Raw está vazia.")
            
        df_socios_silver = df_socios_raw.select(
            col("_c0").alias("cnpj"),
            col("_c1").cast(IntegerType()).alias("tipo_socio"),
            col("_c2").alias("nome_socio"),
            col("_c3").alias("documento_socio"),
            col("_c4").alias("codigo_qualificacao_socio")
        )
        
        # --- PONTO DE VERIFICAÇÃO ---
        logging.info("Amostra de dados de Sócios (Camada Silver):")
        df_socios_silver.printSchema()
        df_socios_silver.show(5, truncate=False)
        # ---------------------------
        
        df_socios_silver.write.format("delta").mode("overwrite").save(str(SILVER_PATH / "socios"))
        
        logging.info("--- Camada Silver criada com sucesso ---")

    except Exception as e:
        logging.error(f"FALHA CRÍTICA na criação da camada Silver: {e}")
        raise e
    