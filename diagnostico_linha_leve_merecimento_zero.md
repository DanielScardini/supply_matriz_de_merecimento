# 🔍 Diagnóstico Específico: LINHA LEVE com Merecimento Zero

## 🎯 Problema Identificado
Um grupo de necessidade da **DIRETORIA LINHA LEVE** tem **todos os merecimentos = 0** mesmo existindo **muita demanda**.

## 📋 Configuração para LINHA LEVE

### Regra de Agrupamento:
```python
"DIRETORIA LINHA LEVE": {
    "coluna_grupo_necessidade": "NmEspecieGerencial",
    "tipo_agrupamento": "espécie_gerencial", 
    "descricao": "Agrupamento por espécie gerencial + voltagem (DsVoltagem)"
}
```

### Formação do grupo_de_necessidade:
```python
grupo_de_necessidade = NmEspecieGerencial + "_" + DsVoltagem
```
- **DsVoltagem** nulls são preenchidos com `""`
- **DsVoltagem** é truncada para 3 caracteres: `F.substring(..., 1, 3)`

## 🚨 **Possíveis Causas Específicas para LINHA LEVE:**

### 1. **NmEspecieGerencial NULL ou "SEM_GN"**
```python
F.coalesce(F.col("NmEspecieGerencial"), F.lit("SEM_GN"))
```
**Problema**: Se `NmEspecieGerencial` é NULL, vira "SEM_GN_" + voltagem
**Resultado**: Grupo pode ser filtrado ou não ter dados históricos suficientes

### 2. **DsVoltagem NULL Gerando Grupos Estranhos**
```python
F.substring(F.coalesce(F.col("DsVoltagem"), F.lit("")), 1, 3)
```
**Problema**: Se `DsVoltagem` é NULL, vira apenas `NmEspecieGerencial + "_"`
**Resultado**: Grupos podem ficar mal formados

### 3. **Join com Tabela de Grupos Perdendo Dados**
**Localização**: Análises factuais usam:
```python
.join(
    spark.table("databox.bcg_comum.supply_grupo_de_necessidade_linha_leve"),
    on="CdSku",
    how="left"
)
.filter(F.col("grupo_de_necessidade").isNotNull())
```
**Problema**: Se SKUs não existem na tabela de grupos, são perdidos

### 4. **Filtro Específico por Setor**
**Localização**: Análises aplicam:
```python
.filter(F.col("NmSetorGerencial") == 'PORTATEIS')
```
**Problema**: Se o grupo não é PORTATEIS, é excluído das análises

### 5. **Tabela de Grupos Desatualizada**
A tabela `databox.bcg_comum.supply_grupo_de_necessidade_linha_leve` é criada nas análises:
```python
df_grupo_de_necessidade_leves = (
    df_merecimento_offline['LINHA_LEVE']
    .select("CdSku", "grupo_de_necessidade")
    .distinct()
)
df_grupo_de_necessidade_leves.write.mode("overwrite").saveAsTable(...)
```
**Problema**: Se a matriz não foi recalculada, a tabela pode estar desatualizada

## 🔧 **Plano de Investigação Específico para LINHA LEVE**

### Etapa 1: Verificar Formação do Grupo
```python
# Verificar como o grupo está sendo formado
df_base = spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
df_linha_leve = df_base.filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA LINHA LEVE')

# Verificar valores de NmEspecieGerencial e DsVoltagem
df_valores = df_linha_leve.select(
    "CdSku",
    "NmEspecieGerencial", 
    "DsVoltagem",
    F.coalesce(F.col("NmEspecieGerencial"), F.lit("SEM_GN")).alias("NmEspecie_filled"),
    F.substring(F.coalesce(F.col("DsVoltagem"), F.lit("")), 1, 3).alias("DsVoltagem_filled"),
    F.concat(
        F.coalesce(F.col("NmEspecieGerencial"), F.lit("SEM_GN")),
        F.lit("_"),
        F.substring(F.coalesce(F.col("DsVoltagem"), F.lit("")), 1, 3)
    ).alias("grupo_formado")
).distinct()

print("Grupos formados:")
df_valores.groupBy("grupo_formado").count().orderBy(F.desc("count")).show(50)

# Verificar se o grupo problema existe
grupo_problema = "SEU_GRUPO_AQUI"  # Substitua pelo grupo com problema
df_grupo_problema = df_valores.filter(F.col("grupo_formado") == grupo_problema)
print(f"SKUs no grupo {grupo_problema}: {df_grupo_problema.count()}")
```

### Etapa 2: Verificar Tabela de Grupos
```python
# Verificar se o grupo existe na tabela de mapeamento
df_grupos_tabela = spark.table("databox.bcg_comum.supply_grupo_de_necessidade_linha_leve")
print(f"Total de grupos na tabela: {df_grupos_tabela.select('grupo_de_necessidade').distinct().count()}")

# Verificar se o grupo problema está na tabela
grupo_na_tabela = df_grupos_tabela.filter(F.col("grupo_de_necessidade") == grupo_problema)
print(f"Grupo {grupo_problema} na tabela: {grupo_na_tabela.count()} SKUs")

# Verificar SKUs órfãos (que não estão na tabela de grupos)
skus_linha_leve = df_linha_leve.select("CdSku").distinct()
skus_sem_grupo = skus_linha_leve.join(df_grupos_tabela, on="CdSku", how="left_anti")
print(f"SKUs sem grupo de necessidade: {skus_sem_grupo.count()}")
```

### Etapa 3: Verificar Filtros Aplicados
```python
# Verificar se o grupo está sendo filtrado por setor
df_com_setor = df_linha_leve.join(
    spark.table('data_engineering_prd.app_venda.mercadoria')
    .select(F.col("CdSkuLoja").alias("CdSku"), "NmSetorGerencial"),
    on="CdSku", how="left"
)

df_grupo_setor = df_com_setor.filter(F.col("grupo_formado") == grupo_problema)
print("Setores do grupo problema:")
df_grupo_setor.groupBy("NmSetorGerencial").count().show()

# Verificar quantos ficam após filtro PORTATEIS
df_portateis = df_grupo_setor.filter(F.col("NmSetorGerencial") == 'PORTATEIS')
print(f"Registros após filtro PORTATEIS: {df_portateis.count()}")
```

### Etapa 4: Verificar Demanda vs Merecimento
```python
# Verificar demanda total do grupo
demanda_grupo = df_linha_leve.filter(F.col("grupo_formado") == grupo_problema).agg(
    F.sum("QtMercadoria").alias("total_qtmercadoria"),
    F.sum("deltaRuptura").alias("total_deltaruptura"),
    F.count("*").alias("total_registros")
).collect()[0]

print(f"Demanda do grupo {grupo_problema}:")
print(f"  QtMercadoria: {demanda_grupo.total_qtmercadoria}")
print(f"  deltaRuptura: {demanda_grupo.total_deltaruptura}")
print(f"  Total registros: {demanda_grupo.total_registros}")

# Verificar matriz calculada
df_matriz = spark.table("databox.bcg_comum.supply_matriz_merecimento_LINHA_LEVE_teste0110")
merecimento_grupo = df_matriz.filter(F.col("grupo_de_necessidade") == grupo_problema)
print(f"Registros na matriz: {merecimento_grupo.count()}")

if merecimento_grupo.count() > 0:
    stats = merecimento_grupo.agg(
        F.avg("Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura").alias("media_merecimento"),
        F.max("Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura").alias("max_merecimento"),
        F.sum("Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura").alias("soma_merecimento")
    ).collect()[0]
    
    print(f"Estatísticas do merecimento:")
    print(f"  Média: {stats.media_merecimento}")
    print(f"  Máximo: {stats.max_merecimento}")
    print(f"  Soma: {stats.soma_merecimento}")
```

## 🎯 **Soluções Recomendadas**

1. **Recalcular tabela de grupos**:
```python
# Recriar a tabela de grupos atualizada
df_grupos_atualizados = executar_calculo_matriz_merecimento_completo("DIRETORIA LINHA LEVE")
df_grupos_novos = df_grupos_atualizados.select("CdSku", "grupo_de_necessidade").distinct()
df_grupos_novos.write.mode("overwrite").saveAsTable("databox.bcg_comum.supply_grupo_de_necessidade_linha_leve")
```

2. **Verificar se grupo não está sendo filtrado** por setor incorretamente

3. **Validar formação do grupo** - pode estar com caracteres especiais ou formatação incorreta

4. **Verificar se há dados suficientes** para médias aparadas (mínimo 10 observações)

## 🚨 **Suspeita Principal**
O problema mais provável é que o grupo está sendo **perdido no join** com a tabela `supply_grupo_de_necessidade_linha_leve` ou está sendo **filtrado pelo setor** (só PORTATEIS são analisados).
