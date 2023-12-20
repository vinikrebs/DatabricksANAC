# Databricks notebook source
# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/ANAC/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt//ANAC/Prata

# COMMAND ----------

df = spark.read.parquet('dbfs:/mnt/ANAC/Prata/anac_silver.parquet/')
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

#Selecionar colunas solicitadas

colunas = ['Aerodromo_de_Destino', 'Aerodromo_de_Origem', 'Classificacao_da_Ocorrência', 'Danos_a_Aeronave', 'Data_da_Ocorrencia', 'Municipio' , 'UF', 'Regiao', 'Tipo_de_Aerodromo', 'Lesoes_Desconhecidas_Passageiros', 'Lesoes_Desconhecidas_Terceiros', 'Lesoes_Desconhecidas_Tripulantes', 'Lesoes_Fatais_Passageiros', 'Lesoes_Fatais_Terceiros', 'Lesoes_Fatais_Tripulantes', 'Lesoes_Graves_Passageiros', 'Lesoes_Graves_Terceiros', 'Lesoes_Graves_Tripulantes', 'Lesoes_Leves_Passageiros', 'Lesoes_Leves_Terceiros', 'Lesoes_Leves_Tripulantes']

df = df.select(colunas)
display(df)

# COMMAND ----------

#Somar as lesões 

colunas_somar = ['Lesoes_Desconhecidas_Passageiros', 'Lesoes_Desconhecidas_Terceiros', 'Lesoes_Desconhecidas_Tripulantes', 'Lesoes_Fatais_Passageiros', 'Lesoes_Fatais_Terceiros', 'Lesoes_Fatais_Tripulantes', 'Lesoes_Graves_Passageiros', 'Lesoes_Graves_Terceiros', 'Lesoes_Graves_Tripulantes', 'Lesoes_Leves_Passageiros', 'Lesoes_Leves_Terceiros', 'Lesoes_Leves_Tripulantes']
df = df.withColumn("Total_Lesoes", sum(df[somartudo] for somartudo in colunas_somar))
display(df)

# COMMAND ----------

#Renomear Colunas

df = df\
    .withColumnRenamed('Aerodromo_de_Destino', 'Destino')
display(df)

# COMMAND ----------

#Excluir dados Sem Registro
excluir = ["Indeterminado", "Sem Registro", "Exterior"]
df= df.filter(~df['UF'].isin(excluir))
display(df)


# COMMAND ----------

#Coluna com a Data de Atualização dos dados
from pyspark.sql.functions import current_timestamp, date_format, from_utc_timestamp
df = df.withColumn("Atualizacao",\
    date_format(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'), "yyyy-MM-dd HH:mm:ss"))
display(df)

# COMMAND ----------

#Salvar na camada por Estado
df.write\
    .mode("overwrite")\
    .format("parquet")\
    .partitionBy("UF")\
    .save("dbfs:/mnt/ANAC/Ouro/Anac_Gold_Particionado")

# COMMAND ----------


