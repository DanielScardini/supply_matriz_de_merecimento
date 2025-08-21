"""
Módulo para processamento e limpeza de dados.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class ProcessingConfig:
    """Configuração para processamento de dados."""
    
    # Parâmetros para tratamento de outliers
    outlier_threshold: float = 3.0  # Desvios padrão para considerar outlier
    
    # Parâmetros para filtros de vendas
    min_sales_threshold: int = 1  # Venda mínima para considerar válida
    max_sales_threshold: int = 10000  # Venda máxima para considerar válida
    
    # Parâmetros para agrupamento temporal
    moving_average_windows: List[int] = None
    
    def __post_init__(self):
        if self.moving_average_windows is None:
            self.moving_average_windows = [3, 6, 9, 12]


class DataProcessor:
    """Processador de dados para limpeza e preparação."""
    
    def __init__(self, config: ProcessingConfig):
        self.config = config
    
    def clean_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa dados de vendas removendo anomalias e outliers.
        
        Args:
            df: DataFrame com dados de vendas brutos
            
        Returns:
            DataFrame com dados limpos
        """
        if df.empty:
            return df
        
        # Remove vendas com valores extremos
        df_clean = df[
            (df['sales_amount'] >= self.config.min_sales_threshold) &
            (df['sales_amount'] <= self.config.max_sales_threshold)
        ].copy()
        
        # Remove outliers usando método estatístico
        df_clean = self._remove_outliers(df_clean, 'sales_amount')
        
        # Remove registros com dados faltantes críticos
        df_clean = df_clean.dropna(subset=['sku', 'store_id', 'sales_date'])
        
        return df_clean
    
    def _remove_outliers(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Remove outliers de uma coluna específica.
        
        Args:
            df: DataFrame a ser processado
            column: Nome da coluna para análise
            
        Returns:
            DataFrame sem outliers
        """
        if column not in df.columns:
            return df
        
        # Calcula limites para outliers
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        # Filtra outliers
        df_clean = df[
            (df[column] >= lower_bound) & 
            (df[column] <= upper_bound)
        ]
        
        return df_clean
    
    def aggregate_sales_by_period(self, df: pd.DataFrame, 
                                 group_columns: List[str], 
                                 date_column: str = 'sales_date') -> pd.DataFrame:
        """
        Agrega vendas por período e grupo.
        
        Args:
            df: DataFrame com dados de vendas
            group_columns: Colunas para agrupamento
            date_column: Coluna de data
            
        Returns:
            DataFrame agregado
        """
        if df.empty:
            return df
        
        # Converte coluna de data se necessário
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column])
        
        # Agrega vendas
        agg_df = df.groupby(group_columns + [df[date_column].dt.to_period('M')]).agg({
            'sales_amount': ['sum', 'count', 'mean'],
            'sales_quantity': ['sum', 'count', 'mean']
        }).reset_index()
        
        # Renomeia colunas
        agg_df.columns = [col[0] if col[1] == '' else f"{col[0]}_{col[1]}" 
                         for col in agg_df.columns]
        
        return agg_df
    
    def calculate_moving_averages(self, df: pd.DataFrame, 
                                value_column: str, 
                                group_columns: List[str]) -> pd.DataFrame:
        """
        Calcula médias móveis para diferentes janelas temporais.
        
        Args:
            df: DataFrame com dados agregados
            value_column: Coluna com valores para cálculo
            group_columns: Colunas para agrupamento
            
        Returns:
            DataFrame com médias móveis calculadas
        """
        if df.empty:
            return df
        
        df_result = df.copy()
        
        # Ordena por data para cada grupo
        df_result = df_result.sort_values(group_columns + ['sales_date'])
        
        # Calcula médias móveis para cada janela
        for window in self.config.moving_average_windows:
            col_name = f"ma_{window}m"
            df_result[col_name] = df_result.groupby(group_columns)[value_column].rolling(
                window=window, min_periods=1
            ).mean().reset_index(0, drop=True)
        
        return df_result
    
    def filter_abnormal_periods(self, df: pd.DataFrame, 
                              value_column: str, 
                              group_columns: List[str]) -> pd.DataFrame:
        """
        Filtra períodos com demanda anormal (eventos).
        
        Args:
            df: DataFrame com dados agregados
            value_column: Coluna com valores para análise
            group_columns: Colunas para agrupamento
            
        Returns:
            DataFrame sem períodos anormais
        """
        if df.empty:
            return df
        
        # Calcula estatísticas por grupo
        stats = df.groupby(group_columns)[value_column].agg(['mean', 'std']).reset_index()
        
        # Identifica períodos anormais (mais de 2 desvios padrão da média)
        df_result = df.merge(stats, on=group_columns, suffixes=('', '_stats'))
        
        # Filtra períodos normais
        df_clean = df_result[
            (df_result[value_column] >= df_result['mean'] - 2 * df_result['std']) &
            (df_result[value_column] <= df_result['mean'] + 2 * df_result['std'])
        ].copy()
        
        # Remove colunas de estatísticas
        df_clean = df_clean.drop(columns=['mean', 'std'])
        
        return df_clean
