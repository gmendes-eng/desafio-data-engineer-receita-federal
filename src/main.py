# src/main.py

import logging
from pyspark.sql import SparkSession
from ingestion import executar_ingestao
from transformations import bronze_para_raw, transformar_dados_silver, transformar_dados_gold
from database import carregar_dados_no_banco

# Configura o logging para um formato claro, com data e nível da mensagem
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """
    Função principal que orquestra a execução do pipeline de dados,
    com tratamento de erros e utilizando o formato Delta Lake.
    """
    spark = None # Inicializa a variável spark
    try:
        # Adiciona os pacotes do Delta Lake e do PostgreSQL à configuração do Spark.
        spark = SparkSession.builder \
            .appName("PipelineReceitaFederal") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.2.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .master("local[*]") \
            .getOrCreate()

        logging.info("Pipeline iniciado. SparkSession com Delta Lake criada.")

        # --- ETAPAS DO PIPELINE ---

        # 1. BRONZE (LANDING): Baixa os .zip e descompacta os CSVs na área de "pouso".
        executar_ingestao()
        
        # 2. BRONZE (RAW): Converte os CSVs brutos para o formato Delta na área "raw".
        bronze_para_raw(spark)
        
        # 3. SILVER: Lê da camada Raw(Delta), limpa, transforma e salva na camada Silver(Delta).
        transformar_dados_silver(spark)
        
        # 4. GOLD: Lê da camada Silver(Delta), aplica regras de negócio e salva na camada Gold(Delta).
        df_gold = transformar_dados_gold(spark)

        # 5. LOAD: Carrega o DataFrame final da camada Gold no banco de dados PostgreSQL.
        carregar_dados_no_banco(df_gold)

        logging.info("PIPELINE CONCLUÍDO COM SUCESSO!")

    except Exception as e:
        logging.error(f"PIPELINE FALHOU. Erro: {e}")
        # Relança a exceção para que o Docker retorne um status de erro
        raise e
    finally:
        # Garante que a sessão do Spark seja encerrada, mesmo se ocorrer um erro
        if spark:
            spark.stop()
            logging.info("SparkSession encerrada.")

if __name__ == "__main__":
    main()