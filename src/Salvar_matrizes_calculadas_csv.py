# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any

# Inicialização do Spark
spark = SparkSession.builder.appName("salvar_matrizes_merecimento_unificadas").getOrCreate()

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

categorias_list = [
    "DIRETORIA DE TELAS",
    "DIRETORIA TELEFONIA CELULAR", 
    #...
    ]

# COMMAND ----------

for categoria in categorias_list:
    ## TODO SALVAR TABELAS EM ARQUIVOS CSV COM DATA DE HOJE
  print(categoria)
