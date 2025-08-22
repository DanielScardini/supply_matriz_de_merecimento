# Databricks notebook source


# COMMAND ----------

df_vendas_estoque = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
)

df_vendas_estoque.display()
