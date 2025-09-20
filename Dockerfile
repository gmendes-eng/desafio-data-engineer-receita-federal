# Dockerfile (VERSÃO CORRIGIDA)

# Passo 1: Imagem base com Python
FROM python:3.12-slim

# Passo 2: Instalar o Java (dependência do PySpark) e outras utilidades
# - apt-get update: Atualiza a lista de pacotes disponíveis
# - apt-get install -y ...: Instala o Java Development Kit (default-jdk)
# - ENV JAVA_HOME: Define a variável de ambiente para que o Spark encontre o Java
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean
ENV JAVA_HOME /usr/lib/jvm/default-java

# Passo 3: Definir o diretório de trabalho dentro do container
WORKDIR /app

# Passo 4: Copiar o arquivo de dependências
COPY requirements.txt .

# Passo 5: Instalar as dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Passo 6: Copiar todo o nosso código fonte para o container
COPY ./src ./src

# Passo 7: Definir o comando que será executado quando o container iniciar
CMD ["python", "src/main.py"]