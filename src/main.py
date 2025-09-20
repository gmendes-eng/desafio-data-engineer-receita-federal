# src/main.py

from pyspark.sql import SparkSession

# Importaremos as funções dos outros módulos aqui
# from ingestion import executar_ingestao
# from transformations import executar_transformacao_silver, executar_transformacao_gold
# from database import carregar_dados_no_banco

def main():
    """
    Função principal que orquestra a execução do pipeline de dados.
    """
    # 1. Inicializa a SparkSession
    # A SparkSession é o ponto de entrada para qualquer funcionalidade do Spark.
    spark = SparkSession.builder \
        .appName("PipelineReceitaFederal") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .master("local[*]") \
        .getOrCreate()

    print("Pipeline iniciado. SparkSession criada.")

    # --- ETAPA DE INGESTÃO (CAMADA BRONZE) ---
    # Aqui chamaremos a função para baixar e descompactar os dados.
    # executar_ingestao()
    print("ETAPA BRONZE: Ingestão de dados concluída (simulação).")


    # --- ETAPA DE TRANSFORMAÇÃO (CAMADA SILVER) ---
    # Aqui chamaremos a função para limpar e estruturar os dados.
    # df_empresas_silver, df_socios_silver = executar_transformacao_silver(spark)
    print("ETAPA SILVER: Transformação de dados concluída (simulação).")


    # --- ETAPA DE TRANSFORMAÇÃO (CAMADA GOLD) ---
    # Aqui chamaremos a função para aplicar as regras de negócio.
    # df_gold = executar_transformacao_gold(df_empresas_silver, df_socios_silver)
    print("ETAPA GOLD: Aplicação das regras de negócio concluída (simulação).")


    # --- ETAPA DE CARREGAMENTO (LOAD) ---
    # Aqui chamaremos a função para salvar o resultado no PostgreSQL.
    # carregar_dados_no_banco(df_gold)
    print("ETAPA DE CARREGAMENTO: Dados salvos no PostgreSQL (simulação).")


    print("Pipeline concluído com sucesso!")

    # Encerra a SparkSession
    spark.stop()


if __name__ == "__main__":
    main()