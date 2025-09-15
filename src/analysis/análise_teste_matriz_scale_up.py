# Databricks notebook source
# MAGIC %sql SELECT * FROM databox.bcg_comum.supply_matriz_merecimento_LINHA_LEVE_online_teste1509
# MAGIC
# MAGIC WHERE grupo_de_necessidade = 'LIQUIDIFICADORES ACIMA 1001 W._220V                                    '
# MAGIC OR grupo_de_necessidade = 'LIQUIDIFICADORES ACIMA 1001 W._110V                                    '
