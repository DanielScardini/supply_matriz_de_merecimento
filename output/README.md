# Pasta de Saída - Matrizes de Merecimento

Esta pasta contém os arquivos CSV exportados das matrizes de merecimento calculadas pelo sistema unificado.

## Estrutura dos Arquivos

Os arquivos seguem o padrão de nomenclatura:
```
matriz_de_merecimento_{grupo_apelido}_{data_exportacao}_{canal}.csv
```

### Exemplos:
- `matriz_de_merecimento_telas_2024-01-15_offline.csv`
- `matriz_de_merecimento_telefonia_2024-01-15_online.csv`
- `matriz_de_merecimento_linha_branca_2024-01-15_offline.csv`

## Campos dos Arquivos

Cada arquivo CSV contém as seguintes colunas:

| Campo | Descrição |
|-------|-----------|
| `CdFilial` | Código da filial |
| `NmFilial` | Nome da filial |
| `NmPorteLoja` | Porte da loja |
| `NmRegiaoGeografica` | Região geográfica |
| `CdSku` | Código do SKU |
| `grupo_de_necessidade` | Grupo de necessidade do produto |
| `Merecimento_Percentual_{canal}` | Percentual de merecimento normalizado |

## Canais Disponíveis

- **offline**: Canais físicos (lojas)
- **online**: Canais digitais (e-commerce)

## Categorias Suportadas

- **telas**: DIRETORIA DE TELAS
- **telefonia**: DIRETORIA TELEFONIA CELULAR
- **linha_branca**: DIRETORIA LINHA BRANCA
- **linha_leve**: DIRETORIA LINHA LEVE
- **info_games**: DIRETORIA INFO/GAMES

## Como Usar

### 1. Exportação Individual
```python
# Para uma categoria específica
arquivos = executar_exportacao_completa("DIRETORIA DE TELAS")
```

### 2. Exportação em Massa
```python
# Para todas as categorias
resultados = exportar_todas_categorias()
```

### 3. Data Personalizada
```python
# Com data específica
arquivos = executar_exportacao_completa("DIRETORIA DE TELAS", "2024-01-15")
```

## Observações Importantes

- Os arquivos são salvos com `coalesce(1)` para garantir um único arquivo CSV
- O merecimento é normalizado por SKU (soma = 100%)
- Filtros automáticos removem produtos "FORA DE LINHA" e "SEM_GN"
- A pasta é criada automaticamente se não existir

## Manutenção

- Arquivos antigos são sobrescritos automaticamente
- Recomenda-se fazer backup periódico dos arquivos importantes
- A pasta pode ser limpa periodicamente para economizar espaço
