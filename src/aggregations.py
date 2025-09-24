# src/aggregations.py

import logging
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, max as spark_max


SILVER_PATH = Path("data/silver")
GOLD_PATH = Path("data/gold")

def executar_agregacao_gold(spark: SparkSession) -> DataFrame:
    """
    CAMADA GOLD:
    Lê os dados da camada Silver, aplica as regras de negócio e agregações
    para criar a tabela final na camada Gold.
    """
    logging.info("Iniciando a criação da Camada Gold...")

    try:
        # 1. Carrega as tabelas já limpas e prontas para a modelagem
        logging.info("Lendo tabelas da camada Silver...")
        df_empresas = spark.read.format("delta").load(str(SILVER_PATH / "empresas"))
        df_socios = spark.read.format("delta").load(str(SILVER_PATH / "socios"))

        # 2. REGRA 1: Calcular a quantidade de sócios e se há sócio estrangeiro
        logging.info("Aplicando regra de negócio: contagem de sócios e flag de estrangeiro...")
        df_socios_agg = df_socios.groupBy("cnpj").agg(
            # 1.1: Conta o número de sócios para cada CNPJ
            count("nome_socio").alias("qtde_socios"),
            # 1.2: Cria a flag 'flag_socio_estrangeiro'. O tipo 3 identifica sócio estrangeiro.
            spark_max(when(col("tipo_socio") == 3, True).otherwise(False)).alias("flag_socio_estrangeiro")
        )

        # 3. Junta os dados de empresas com os dados agregados de sócios
        df_gold = df_empresas.join(df_socios_agg, "cnpj", "left")
        
        # 4. Garante que empresas sem sócios não fiquem com valores nulos nessas colunas
        df_gold = df_gold.fillna(0, subset=["qtde_socios"])
        df_gold = df_gold.fillna(False, subset=["flag_socio_estrangeiro"])

        # 5. REGRA 2: Calcular a coluna 'doc_alvo'
        # True se o porte da empresa for '03' (PEQUENO PORTE) e a quantidade de sócios for maior que 1.
        logging.info("Aplicando regra de negócio: cálculo do 'doc_alvo'...")
        df_gold = df_gold.withColumn("doc_alvo",
            when((col("cod_porte") == "03") & (col("qtde_socios") > 1), True)
            .otherwise(False)
        )

        # 6. Seleciona apenas as colunas finais para o output do desafio
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

        # 7. Salva o resultado em formato Delta para auditoria e reuso
        df_gold_final.write.format("delta").mode("overwrite").save(str(GOLD_PATH / "resultado_final"))
        logging.info("Dados da camada Gold salvos em formato Delta.")
        
        return df_gold_final

    except Exception as e:
        logging.error(f"FALHA CRÍTICA na criação da camada Gold: {e}")
        raise e