# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/ANAC/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/ANAC/Bronze

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/ANAC/Bronze/V_OCORRENCIA_AMPLA.json")
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tratamento de dados
# MAGIC ####Substituindo colunas de texto null para 'Sem Regitro"
# MAGIC

# COMMAND ----------

colunas = ['Aerodromo_de_Destino', 'Aerodromo_de_Origem', 'CLS', 'Categoria_da_Aeronave', 'Classificacao_da_Ocorrência', 'Danos_a_Aeronave', 'Data_da_Ocorrencia', 'Descricao_do_Tipo', 'Fase_da_Operacao', 'Historico', 'Hora_da_Ocorrência', 'ICAO', 'Ilesos_Passageiros', 'Ilesos_Tripulantes', 'Latitude','Longitude', 'Matricula', 'Modelo', 'Municipio', 'Nome_do_Fabricante', 'Numero_da_Ficha', 'Numero_da_Ocorrencia', 'Numero_de_Assentos', 'Operacao', 'Operador', 'Operador_Padronizado', 'PMD', 'PSSO', 'Regiao', 'Tipo_ICAO', 'Tipo_de_Aerodromo', 'Tipo_de_Ocorrencia', 'UF']

#Percorrer todas as colunas e fazer a mesma coisa pra todas selecionadas na variável 

for ajuste in colunas:
    df = df.fillna('Sem Registro', subset=[ajuste])

display(df)

# COMMAND ----------

coluna_converter = [ 'Lesoes_Desconhecidas_Passageiros', 'Lesoes_Desconhecidas_Terceiros', 'Lesoes_Desconhecidas_Tripulantes', 'Lesoes_Fatais_Passageiros', 'Lesoes_Fatais_Terceiros', 'Lesoes_Fatais_Tripulantes', 'Lesoes_Graves_Passageiros', 'Lesoes_Graves_Terceiros', 'Lesoes_Graves_Tripulantes', 'Lesoes_Leves_Passageiros', 'Lesoes_Leves_Terceiros', 'Lesoes_Leves_Tripulantes']

for converter in coluna_converter:
    df = df\
        .withColumn(converter, df[converter].cast("int"))\
        .fillna(0, subset=[converter])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvar na Pasta Silver em formato parquet

# COMMAND ----------

df.write.mode("overwrite").parquet("dbfs:/mnt/ANAC/Prata/anac_silver.parquet")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/ANAC/Prata

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/ANAC/Prata/anac_silver.parquet/"))

# COMMAND ----------


