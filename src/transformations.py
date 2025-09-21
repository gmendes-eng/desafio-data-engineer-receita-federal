# src/transformations.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, count, when, max as spark_max
from pyspark.sql.types import IntegerType, FloatType

def transformar_dados_silver(spark: SparkSession):
    """
    CAMADA SILVER:
    - Lê os dados brutos da camada Bronze.
    - Limpa e padroniza os nomes das colunas.
    - Converte os tipos de dados para o formato correto.
    - Salva o resultado em formato Parquet.
    """
    print("Iniciando a transformação de dados para a Camada Silver...")

    bronze_path = "data/bronze/"
    silver_path = "data/silver/"

    # ----- Processamento de Empresas -----
    print("Processando dados de empresas para Silver...")
    try:
        # Lê o arquivo CSV bruto da camada bronze. O '*' é um coringa para pegar qualquer arquivo que termine com EMPRECSV.
        # Os arquivos da Receita usam ';' como separador e não possuem cabeçalho.
        df_empresas_raw = spark.read.csv(f"{bronze_path}*EMPRECSV", sep=';', inferSchema=False)

        # Mapeia as colunas brutas (ex: _c0, _c1) para nomes padronizados, seguindo o layout oficial.
        schema_empresas = {
            "_c0": "CNPJ_BASICO",
            "_c1": "RAZAO_SOCIAL_NOME_EMPRESARIAL",
            "_c2": "NATUREZA_JURIDICA",
            "_c3": "QUALIFICACAO_DO_RESPONSAVEL",
            "_c4": "CAPITAL_SOCIAL_DA_EMPRESA",
            "_c5": "PORTE_DA_EMPRESA",
            "_c6": "ENTE_FEDERATIVO_RESPONSAVEL"
        }

        # Itera sobre o dicionário para renomear todas as colunas do DataFrame.
        for old_name, new_name in schema_empresas.items():
            df_empresas_raw = df_empresas_raw.withColumnRenamed(old_name, new_name)

        # Seleciona apenas as colunas necessárias para o desafio, renomeia para nomes mais simples (alias) e converte os tipos de dados.
        df_empresas_silver = df_empresas_raw.select(
            col("CNPJ_BASICO").alias("cnpj"),
            col("RAZAO_SOCIAL_NOME_EMPRESARIAL").alias("razao_social"),
            col("NATUREZA_JURIDICA").cast(IntegerType()).alias("natureza_juridica"),
            col("QUALIFICACAO_DO_RESPONSAVEL").cast(IntegerType()).alias("qualificacao_responsavel"),
            # O capital social vem com vírgula como separador decimal (ex: "1000,50"). Substituímos por ponto para converter para float.
            regexp_replace(col("CAPITAL_SOCIAL_DA_EMPRESA"), ",", ".").cast(FloatType()).alias("capital_social"),
            col("PORTE_DA_EMPRESA").alias("cod_porte")
        )

        # --- PONTO DE VERIFICAÇÃO ADICIONADO ---
        print("Amostra de dados de Empresas (Camada Silver):")
        df_empresas_silver.printSchema()
        df_empresas_silver.show(10, truncate=False)
        # ------------------------------------

        # Salva o DataFrame limpo em formato Parquet, que é otimizado para leitura e análise.
        # O modo "overwrite" garante que os dados antigos sejam substituídos a cada execução.
        df_empresas_silver.write.mode("overwrite").parquet(f"{silver_path}empresas")
        print("Dados de empresas salvos na camada Silver.")
    except Exception as e:
        print(f"Erro ao processar dados de empresas para Silver: {e}")

    # ----- Processamento de Sócios -----
    print("Processando dados de sócios para Silver...")
    try:
        # Lê o arquivo CSV bruto de sócios.
        df_socios_raw = spark.read.csv(f"{bronze_path}*SOCIOCSV", sep=';', inferSchema=False)

        # Mapeia as colunas brutas para nomes padronizados.
        schema_socios = {
            "_c0": "CNPJ_BASICO",
            "_c1": "IDENTIFICADOR_DE_SOCIO",
            "_c2": "NOME_DO_SOCIO_OU_RAZAO_SOCIAL",
            "_c3": "CNPJ_CPF_DO_SOCIO",
            "_c4": "QUALIFICACAO_DO_SOCIO"
        }
        
        # Renomeia as colunas do DataFrame.
        for old_name, new_name in schema_socios.items():
            df_socios_raw = df_socios_raw.withColumnRenamed(old_name, new_name)
        
        # Seleciona, renomeia e converte os tipos de dados.
        df_socios_silver = df_socios_raw.select(
            col("CNPJ_BASICO").alias("cnpj"),
            col("IDENTIFICADOR_DE_SOCIO").cast(IntegerType()).alias("tipo_socio"),
            col("NOME_DO_SOCIO_OU_RAZAO_SOCIAL").alias("nome_socio"),
            col("CNPJ_CPF_DO_SOCIO").alias("documento_socio"),
            col("QUALIFICACAO_DO_SOCIO").alias("codigo_qualificacao_socio")
        )
        
        # --- PONTO DE VERIFICAÇÃO ADICIONADO ---
        print("Amostra de dados de Sócios (Camada Silver):")
        df_socios_silver.printSchema()
        df_socios_silver.show(10, truncate=False)
        # ------------------------------------
        
        # Salva o DataFrame de sócios limpo em formato Parquet na camada Silver.
        df_socios_silver.write.mode("overwrite").parquet(f"{silver_path}socios")
        print("Dados de sócios salvos na camada Silver.")
    except Exception as e:
        print(f"Erro ao processar dados de sócios para Silver: {e}")

    print("Transformação para a Camada Silver concluída.")


def transformar_dados_gold(spark: SparkSession):
    """
    CAMADA GOLD:
    - Lê os dados já limpos da camada Silver.
    - Aplica as regras de negócio específicas do desafio.
    - Salva o resultado em formato Parquet na pasta Gold e retorna o DataFrame.
    """
    print("Iniciando a transformação de dados para a Camada Gold...")
    silver_path = "data/silver/"
    gold_path = "data/gold/"

    # Carrega as tabelas já limpas e estruturadas da camada Silver.
    df_empresas = spark.read.parquet(f"{silver_path}empresas")
    df_socios = spark.read.parquet(f"{silver_path}socios")

    # REGRA 1: CALCULAR A QUANTIDADE DE SÓCIOS E IDENTIFICAR SÓCIO ESTRANGEIRO
    # Agrupamos a tabela de sócios pelo CNPJ para aplicar as agregações.
    df_socios_agg = df_socios.groupBy("cnpj").agg(
        # REGRA 1.1: Contar o número de sócios para cada CNPJ.
        count("nome_socio").alias("qtde_socios"),
        # REGRA 1.2: Criar a flag 'flag_socio_estrangeiro'.
        # O código 3 para 'tipo_socio' identifica um sócio estrangeiro.
        # Usamos spark_max(when(...)) para que, se houver PELO MENOS UM sócio estrangeiro no grupo, o resultado seja True.
        spark_max(when(col("tipo_socio") == 3, True).otherwise(False)).alias("flag_socio_estrangeiro")
    )

    # Junta a tabela de empresas com os dados agregados de sócios, usando o CNPJ como chave.
    # O 'left' join garante que todas as empresas da tabela de empresas sejam mantidas, mesmo que não tenham sócios correspondentes.
    df_gold = df_empresas.join(df_socios_agg, "cnpj", "left")
    
    # Após o join, empresas sem sócios terão valores nulos para as colunas agregadas.
    # Substituímos esses nulos por valores padrão (0 para quantidade, False para a flag).
    df_gold = df_gold.fillna(0, subset=["qtde_socios"])
    df_gold = df_gold.fillna(False, subset=["flag_socio_estrangeiro"])

    # REGRA 2: CALCULAR A COLUNA 'doc_alvo'
    # A regra do desafio é: True quando o porte da empresa for '03' (PEQUENO PORTE) E a quantidade de sócios for maior que 1.
    # O código '03' para PORTE DA EMPRESA corresponde a "EMPRESA DE PEQUENO PORTE".
    df_gold = df_gold.withColumn("doc_alvo",
        when((col("cod_porte") == "03") & (col("qtde_socios") > 1), True)
        .otherwise(False)
    )

    # Seleciona as colunas finais e sobrescreve a variável df_gold  que compõem o output esperado pelo desafio.
    df_gold = df_gold.select(
        "cnpj",
        "qtde_socios",
        "flag_socio_estrangeiro",
        "doc_alvo"
    )

    # --- PONTO DE VERIFICAÇÃO ADICIONADO ---
    print("Amostra de dados Finais (Camada Gold):")
    df_gold.printSchema()
    df_gold.show(10, truncate=False)
    # ------------------------------------

    # Salva o resultado final em formato Parquet na camada Gold para persistência e visualização do uso do modelo medalhão                                                                                    
    df_gold.write.mode("overwrite").parquet(f"{gold_path}resultado_final")
    print("Dados da camada Gold salvos em formato Parquet.")
    
    print("Transformação para a Camada Gold concluída.")
    
    # Retorna o DataFrame final para ser usado na próxima etapa (carregamento no banco de dados).
    return df_gold

    