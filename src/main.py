# src/main.py

from pyspark.sql import SparkSession
from ingestion import executar_ingestao
from transformations import transformar_dados_silver, transformar_dados_gold
from database import carregar_dados_no_banco

def main():
    """
    Função principal que orquestra a execução do pipeline de dados.
    """
    spark = SparkSession.builder \
        .appName("PipelineReceitaFederal") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .master("local[*]") \
        .getOrCreate()

    print("Pipeline iniciado. SparkSession criada.")

    # --- ETAPA DE INGESTÃO (CAMADA BRONZE) ---
    executar_ingestao()

    # --- ETAPA DE TRANSFORMAÇÃO (CAMADA SILVER) ---
    transformar_dados_silver(spark)

    # --- ETAPA DE TRANSFORMAÇÃO (CAMADA GOLD) ---
    # Chama a função Gold e armazena o DataFrame final
    df_gold = transformar_dados_gold(spark)
    # Apenas para ver o resultado no log
    print("Amostra dos dados Gold para verificação:")
    df_gold.show(50, truncate=False) # Para verificação no log

    # --- ETAPA DE CARREGAMENTO (LOAD) ---
    # Chamamos a função final, passando o DataFrame Gold
    carregar_dados_no_banco(df_gold)

    print("Pipeline concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    main()