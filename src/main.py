# src/main.py

import logging
from pyspark.sql import SparkSession
from ingestion import baixar_arquivos_zipados, descompactar_arquivos, converter_csvs_para_delta
from transformations import executar_transformacao_silver
from aggregations import executar_agregacao_gold
from database import carregar_dados_no_banco

# Configura um logging para o orquestrador principal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """
    Orquestrador principal do pipeline de dados da Receita Federal.
    Executa cada camada e etapa da arquitetura Medallion em sequência.
    """
    spark = None
    try:
        # Configuração do Spark, incluindo pacotes para Delta e PostgreSQL
        spark = SparkSession.builder \
            .appName("PipelineReceitaFederal") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.2.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .master("local[*]") \
            .getOrCreate()

        logging.info(">>> PIPELINE INICIADO: SparkSession criada com sucesso.")

        # --- ETAPAS DO PIPELINE ---

        # 1. CAMADA BRONZE (executada em três passos distintos)
        baixar_arquivos_zipados()         # 1.1: Download dos arquivos originais
        descompactar_arquivos()           # 1.2: Extração dos CSVs
        converter_csvs_para_delta(spark)  # 1.3: Conversão para Delta (Raw)

        # 2. CAMADA SILVER
        executar_transformacao_silver(spark)
        
        # 3. CAMADA GOLD
        df_gold = executar_agregacao_gold(spark)
        
        # 4. CARGA NO BANCO DE DADOS
        carregar_dados_no_banco(df_gold)

        logging.info(">>> PIPELINE CONCLUÍDO COM SUCESSO!")

    except Exception as e:
        logging.error(f"!!! PIPELINE FALHOU. Erro: {e}", exc_info=True)
        # Relança a exceção para que o Docker container saia com status de erro
        raise e
    finally:
        if spark:
            spark.stop()
            logging.info("SparkSession encerrada.")

if __name__ == "__main__":
    main()