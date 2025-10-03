# 🔍 Diagnóstico Específico: ASPIRADOR DE PO_220 e ASPIRADOR DE PO_110

## Script de Investigação

```python
# 1. VERIFICAR FORMAÇÃO DOS GRUPOS
print("=" * 80)
print("🔍 INVESTIGANDO GRUPOS: ASPIRADOR DE PO_220 e ASPIRADOR DE PO_110")
print("=" * 80)

# Carregar dados base
df_base = spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
df_linha_leve = df_base.filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA LINHA LEVE')

# Verificar SKUs com NmEspecieGerencial = "ASPIRADOR DE PO"
df_aspiradores = df_linha_leve.filter(F.col("NmEspecieGerencial").like("%ASPIRADOR%"))

print("🔍 SKUs com ASPIRADOR encontrados:")
df_especies_aspirador = df_aspiradores.select("NmEspecieGerencial").distinct().orderBy("NmEspecieGerencial")
df_especies_aspirador.show(truncate=False)

# Verificar voltagens dos aspiradores
print("\n🔍 Voltagens dos aspiradores:")
df_voltagens = df_aspiradores.select("NmEspecieGerencial", "DsVoltagem").distinct().orderBy("NmEspecieGerencial", "DsVoltagem")
df_voltagens.show(truncate=False)

# Formar grupos como no código
df_grupos_formados = df_aspiradores.withColumn(
    "DsVoltagem_filled",
    F.substring(F.coalesce(F.col("DsVoltagem"), F.lit("")), 1, 3)
).withColumn(
    "grupo_de_necessidade",
    F.concat(
        F.coalesce(F.col("NmEspecieGerencial"), F.lit("SEM_GN")),
        F.lit("_"),
        F.col("DsVoltagem_filled")
    )
).select("CdSku", "NmEspecieGerencial", "DsVoltagem", "grupo_de_necessidade").distinct()

print("\n🔍 Grupos de necessidade formados para aspiradores:")
df_grupos_count = df_grupos_formados.groupBy("grupo_de_necessidade").count().orderBy(F.desc("count"))
df_grupos_count.show(truncate=False)

# Verificar especificamente os grupos problema
grupos_problema = ["ASPIRADOR DE PO_220", "ASPIRADOR DE PO_110"]
for grupo in grupos_problema:
    count = df_grupos_formados.filter(F.col("grupo_de_necessidade") == grupo).count()
    print(f"📊 {grupo}: {count} SKUs")

# 2. VERIFICAR TABELA DE GRUPOS DE NECESSIDADE
print("\n" + "=" * 80)
print("🔍 VERIFICANDO TABELA DE GRUPOS DE NECESSIDADE")
print("=" * 80)

try:
    df_grupos_tabela = spark.table("databox.bcg_comum.supply_grupo_de_necessidade_linha_leve")
    
    # Verificar se os grupos problema existem na tabela
    for grupo in grupos_problema:
        count_tabela = df_grupos_tabela.filter(F.col("grupo_de_necessidade") == grupo).count()
        print(f"📊 {grupo} na tabela de grupos: {count_tabela} SKUs")
    
    # Verificar todos os grupos de aspirador na tabela
    aspiradores_tabela = df_grupos_tabela.filter(F.col("grupo_de_necessidade").like("%ASPIRADOR%"))
    print(f"\n📊 Total de grupos ASPIRADOR na tabela: {aspiradores_tabela.select('grupo_de_necessidade').distinct().count()}")
    aspiradores_tabela.groupBy("grupo_de_necessidade").count().orderBy(F.desc("count")).show(truncate=False)
    
except Exception as e:
    print(f"❌ Erro ao acessar tabela de grupos: {e}")

# 3. VERIFICAR SETOR GERENCIAL
print("\n" + "=" * 80)
print("🔍 VERIFICANDO SETOR GERENCIAL DOS ASPIRADORES")
print("=" * 80)

# Join com tabela de mercadoria para pegar setor
df_com_setor = df_aspiradores.join(
    spark.table('data_engineering_prd.app_venda.mercadoria')
    .select(F.col("CdSkuLoja").alias("CdSku"), "NmSetorGerencial", "NmClasseGerencial"),
    on="CdSku", how="left"
)

print("📊 Setores dos aspiradores:")
df_setores = df_com_setor.groupBy("NmSetorGerencial").count().orderBy(F.desc("count"))
df_setores.show(truncate=False)

print("\n📊 Classes dos aspiradores:")
df_classes = df_com_setor.groupBy("NmClasseGerencial").count().orderBy(F.desc("count"))
df_classes.show(truncate=False)

# Verificar quantos aspiradores são PORTATEIS
portateis_count = df_com_setor.filter(F.col("NmSetorGerencial") == 'PORTATEIS').count()
total_count = df_com_setor.count()
print(f"\n📊 Aspiradores no setor PORTATEIS: {portateis_count} de {total_count} ({portateis_count/total_count*100:.1f}%)")

# 4. VERIFICAR DEMANDA DOS GRUPOS PROBLEMA
print("\n" + "=" * 80)
print("🔍 VERIFICANDO DEMANDA DOS GRUPOS PROBLEMA")
print("=" * 80)

for grupo in grupos_problema:
    df_grupo = df_grupos_formados.filter(F.col("grupo_de_necessidade") == grupo)
    skus_grupo = [row.CdSku for row in df_grupo.select("CdSku").collect()]
    
    if skus_grupo:
        # Calcular demanda total do grupo
        df_demanda = df_linha_leve.filter(F.col("CdSku").isin(skus_grupo))
        
        demanda_stats = df_demanda.agg(
            F.sum("QtMercadoria").alias("total_qtmercadoria"),
            F.sum("deltaRuptura").alias("total_deltaruptura"),
            F.count("*").alias("total_registros"),
            F.countDistinct("CdSku").alias("skus_unicos"),
            F.countDistinct("CdFilial").alias("filiais_unicas")
        ).collect()[0]
        
        print(f"\n📊 DEMANDA - {grupo}:")
        print(f"  • QtMercadoria: {demanda_stats.total_qtmercadoria}")
        print(f"  • deltaRuptura: {demanda_stats.total_deltaruptura}")
        print(f"  • Total registros: {demanda_stats.total_registros}")
        print(f"  • SKUs únicos: {demanda_stats.skus_unicos}")
        print(f"  • Filiais únicas: {demanda_stats.filiais_unicas}")
        
        # Verificar se há demanda recente
        demanda_recente = df_demanda.filter(F.col("DtAtual") >= "2025-07-01").agg(
            F.sum("QtMercadoria").alias("qtmercadoria_recente"),
            F.sum("deltaRuptura").alias("deltaruptura_recente")
        ).collect()[0]
        
        print(f"  • Demanda recente (jul/2025+): QtMercadoria={demanda_recente.qtmercadoria_recente}, deltaRuptura={demanda_recente.deltaruptura_recente}")

# 5. VERIFICAR MATRIZ CALCULADA
print("\n" + "=" * 80)
print("🔍 VERIFICANDO MATRIZ CALCULADA")
print("=" * 80)

try:
    df_matriz = spark.table("databox.bcg_comum.supply_matriz_merecimento_LINHA_LEVE_teste0110")
    
    for grupo in grupos_problema:
        df_merecimento = df_matriz.filter(F.col("grupo_de_necessidade") == grupo)
        count_matriz = df_merecimento.count()
        print(f"\n📊 {grupo} na matriz calculada: {count_matriz} registros")
        
        if count_matriz > 0:
            # Verificar estatísticas do merecimento
            stats = df_merecimento.agg(
                F.avg("Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura").alias("media_merecimento"),
                F.max("Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura").alias("max_merecimento"),
                F.min("Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura").alias("min_merecimento"),
                F.sum("Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura").alias("soma_merecimento"),
                F.countDistinct("CdFilial").alias("filiais_com_merecimento")
            ).collect()[0]
            
            print(f"  • Merecimento médio: {stats.media_merecimento}")
            print(f"  • Merecimento máximo: {stats.max_merecimento}")
            print(f"  • Merecimento mínimo: {stats.min_merecimento}")
            print(f"  • Soma merecimentos: {stats.soma_merecimento}")
            print(f"  • Filiais com merecimento: {stats.filiais_com_merecimento}")
            
            # Mostrar algumas linhas
            print(f"  • Primeiras 5 linhas:")
            df_merecimento.select("CdFilial", "Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura").show(5)
        else:
            print(f"  ❌ {grupo} NÃO ENCONTRADO na matriz calculada!")

except Exception as e:
    print(f"❌ Erro ao acessar matriz calculada: {e}")

print("\n" + "=" * 80)
print("🎯 CONCLUSÕES")
print("=" * 80)
print("1. Verifique se os aspiradores estão no setor PORTATEIS")
print("2. Verifique se a tabela de grupos está atualizada")
print("3. Verifique se há dados suficientes para médias aparadas")
print("4. Verifique se os grupos estão sendo filtrados incorretamente")
```

## 🎯 Próximos Passos

Execute esse script no Databricks para descobrir:

1. **Se os grupos estão sendo formados corretamente**
2. **Se estão na tabela de mapeamento**  
3. **Se estão no setor PORTATEIS** (filtro das análises)
4. **Se têm demanda suficiente**
5. **Se aparecem na matriz final**

Minha **suspeita principal** é que os aspiradores **não estão no setor PORTATEIS**, sendo filtrados fora das análises factuais!
