# src/database.py

import os
from pyspark.sql import DataFrame

def carregar_dados_no_banco(df: DataFrame):
    """
    Carrega o DataFrame final da camada Gold em uma tabela no PostgreSQL.
    """
    print("Iniciando o carregamento dos dados no PostgreSQL...")

    try:
        # Pega as credenciais do banco de dados das variáveis de ambiente
        # que definimos no docker-compose.yml
        db_host = os.getenv("POSTGRES_HOST")
        db_port = os.getenv("POSTGRES_PORT")
        db_name = os.getenv("POSTGRES_DB")
        db_user = os.getenv("POSTGRES_USER")
        db_password = os.getenv("POSTGRES_PASSWORD")

        # Monta a URL de conexão JDBC
        jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

        # Define as propriedades da conexão
        jdbc_properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }

        # Define o nome da tabela onde os dados serão salvos
        table_name = "resultado_final_desafio"

        # Escreve o DataFrame na tabela do PostgreSQL
        # O modo "overwrite" garante que, se rodarmos o pipeline de novo,
        # a tabela antiga será substituída pela nova, evitando dados duplicados.
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=jdbc_properties
        )
        
        print(f"Dados carregados com sucesso na tabela '{table_name}' do PostgreSQL!")

    except Exception as e:
        print(f"Erro ao carregar os dados no PostgreSQL: {e}")