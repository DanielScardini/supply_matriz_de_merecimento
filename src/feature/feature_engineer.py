"""
Módulo para engenharia de features para modelo de merecimento.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from sklearn.preprocessing import StandardScaler, LabelEncoder


@dataclass
class FeatureConfig:
    """Configuração para engenharia de features."""
    
    # Parâmetros para cálculo de features temporais
    lookback_periods: List[int] = None  # Períodos para lookback
    forecast_periods: int = 30  # Períodos para previsão
    
    # Parâmetros para features de risco
    stockout_threshold: float = 0.1  # Threshold para considerar risco de ruptura
    demand_volatility_threshold: float = 0.5  # Threshold para volatilidade
    
    # Parâmetros para normalização
    normalize_features: bool = True
    scale_features: bool = True
    
    def __post_init__(self):
        if self.lookback_periods is None:
            self.lookback_periods = [1, 3, 6, 12]


class FeatureEngineer:
    """Engenheiro de features para modelo de merecimento."""
    
    def __init__(self, config: FeatureConfig):
        self.config = config
        self.scaler = StandardScaler() if config.scale_features else None
        self.label_encoders = {}
    
    def create_time_based_features(self, df: pd.DataFrame, 
                                 date_column: str = 'sales_date',
                                 group_columns: List[str] = None) -> pd.DataFrame:
        """
        Cria features baseadas em tempo para cada SKU-loja.
        
        Args:
            df: DataFrame com dados de vendas processados
            date_column: Coluna de data
            group_columns: Colunas para agrupamento (ex: ['sku', 'store_id'])
            
        Returns:
            DataFrame com features temporais
        """
        if df.empty:
            return df
        
        if group_columns is None:
            group_columns = ['sku', 'store_id']
        
        df_result = df.copy()
        df_result[date_column] = pd.to_datetime(df_result[date_column])
        
        # Ordena por data para cada grupo
        df_result = df_result.sort_values(group_columns + [date_column])
        
        # Features de tendência
        for period in self.config.lookback_periods:
            # Média móvel
            df_result[f'trend_ma_{period}m'] = df_result.groupby(group_columns)['sales_amount_sum'].rolling(
                window=period, min_periods=1
            ).mean().reset_index(0, drop=True)
            
            # Crescimento percentual
            df_result[f'growth_{period}m'] = df_result.groupby(group_columns)['sales_amount_sum'].pct_change(periods=period)
            
            # Volatilidade
            df_result[f'volatility_{period}m'] = df_result.groupby(group_columns)['sales_amount_sum'].rolling(
                window=period, min_periods=2
            ).std().reset_index(0, drop=True)
        
        # Features sazonais
        df_result['month'] = df_result[date_column].dt.month
        df_result['quarter'] = df_result[date_column].dt.quarter
        df_result['day_of_week'] = df_result[date_column].dt.dayofweek
        
        return df_result
    
    def create_demand_features(self, df: pd.DataFrame, 
                             group_columns: List[str] = None) -> pd.DataFrame:
        """
        Cria features relacionadas à demanda.
        
        Args:
            df: DataFrame com dados processados
            group_columns: Colunas para agrupamento
            
        Returns:
            DataFrame com features de demanda
        """
        if df.empty:
            return df
        
        if group_columns is None:
            group_columns = ['sku', 'store_id']
        
        df_result = df.copy()
        
        # Demanda média por período
        df_result['demand_mean'] = df_result.groupby(group_columns)['sales_amount_sum'].transform('mean')
        df_result['demand_median'] = df_result.groupby(group_columns)['sales_amount_sum'].transform('median')
        
        # Demanda normalizada
        df_result['demand_normalized'] = df_result['sales_amount_sum'] / df_result['demand_mean']
        
        # Estabilidade da demanda
        df_result['demand_stability'] = 1 / (1 + df_result.groupby(group_columns)['sales_amount_sum'].transform('std'))
        
        # Consistência de vendas
        df_result['sales_consistency'] = df_result.groupby(group_columns)['sales_amount_sum'].transform(
            lambda x: (x > 0).sum() / len(x)
        )
        
        return df_result
    
    def create_risk_features(self, df: pd.DataFrame, 
                           group_columns: List[str] = None) -> pd.DataFrame:
        """
        Cria features relacionadas ao risco de ruptura.
        
        Args:
            df: DataFrame com dados processados
            group_columns: Colunas para agrupamento
            
        Returns:
            DataFrame com features de risco
        """
        if df.empty:
            return df
        
        if group_columns is None:
            group_columns = ['sku', 'store_id']
        
        df_result = df.copy()
        
        # Risco de ruptura baseado na volatilidade
        df_result['rupture_risk'] = np.where(
            df_result['volatility_3m'] > self.config.demand_volatility_threshold,
            1, 0
        )
        
        # Padrão de demanda (consistente vs. esporádico)
        df_result['demand_pattern'] = np.where(
            df_result['sales_consistency'] > 0.8,
            'consistent',
            np.where(df_result['sales_consistency'] > 0.5, 'moderate', 'sporadic')
        )
        
        # Indicador de sazonalidade
        df_result['seasonality_indicator'] = df_result.groupby(group_columns)['sales_amount_sum'].transform(
            lambda x: x.rolling(window=12, min_periods=6).std().fillna(0)
        )
        
        return df_result
    
    def create_store_features(self, df: pd.DataFrame, 
                           stores_df: pd.DataFrame,
                           group_columns: List[str] = None) -> pd.DataFrame:
        """
        Cria features relacionadas às características das lojas.
        
        Args:
            df: DataFrame com dados de vendas
            stores_df: DataFrame com informações das lojas
            group_columns: Colunas para agrupamento
            
        Returns:
            DataFrame com features de loja
        """
        if df.empty or stores_df.empty:
            return df
        
        if group_columns is None:
            group_columns = ['sku', 'store_id']
        
        # Merge com dados de loja
        df_result = df.merge(stores_df, on='store_id', how='left')
        
        # Features de localização (se disponível)
        if 'region' in df_result.columns:
            df_result['region_encoded'] = self._encode_categorical(df_result, 'region')
        
        if 'cluster' in df_result.columns:
            df_result['cluster_encoded'] = self._encode_categorical(df_result, 'cluster')
        
        # Features de tamanho da loja (se disponível)
        if 'store_size' in df_result.columns:
            df_result['store_size_category'] = pd.cut(
                df_result['store_size'],
                bins=3,
                labels=['small', 'medium', 'large']
            )
            df_result['store_size_encoded'] = self._encode_categorical(df_result, 'store_size_category')
        
        return df_result
    
    def create_product_features(self, df: pd.DataFrame, 
                             products_df: pd.DataFrame,
                             group_columns: List[str] = None) -> pd.DataFrame:
        """
        Cria features relacionadas às características dos produtos.
        
        Args:
            df: DataFrame com dados de vendas
            products_df: DataFrame com informações dos produtos
            group_columns: Colunas para agrupamento
            
        Returns:
            DataFrame com features de produto
        """
        if df.empty or products_df.empty:
            return df
        
        if group_columns is None:
            group_columns = ['sku', 'store_id']
        
        # Merge com dados de produto
        df_result = df.merge(products_df, on='sku', how='left')
        
        # Features de categoria (se disponível)
        if 'category' in df_result.columns:
            df_result['category_encoded'] = self._encode_categorical(df_result, 'category')
        
        if 'need_group' in df_result.columns:
            df_result['need_group_encoded'] = self._encode_categorical(df_result, 'need_group')
        
        # Features de preço (se disponível)
        if 'price' in df_result.columns:
            df_result['price_category'] = pd.qcut(
                df_result['price'],
                q=5,
                labels=['very_low', 'low', 'medium', 'high', 'very_high']
            )
            df_result['price_category_encoded'] = self._encode_categorical(df_result, 'price_category')
        
        return df_result
    
    def _encode_categorical(self, df: pd.DataFrame, column: str) -> pd.Series:
        """
        Codifica colunas categóricas usando LabelEncoder.
        
        Args:
            df: DataFrame com dados
            column: Nome da coluna categórica
            
        Returns:
            Série com valores codificados
        """
        if column not in df.columns:
            return pd.Series(dtype='object')
        
        if column not in self.label_encoders:
            self.label_encoders[column] = LabelEncoder()
            # Treina o encoder com valores únicos
            unique_values = df[column].dropna().unique()
            if len(unique_values) > 0:
                self.label_encoders[column].fit(unique_values)
        
        # Codifica os valores
        encoded_values = self.label_encoders[column].transform(
            df[column].fillna('unknown')
        )
        
        return pd.Series(encoded_values, index=df.index)
    
    def normalize_features(self, df: pd.DataFrame, 
                         feature_columns: List[str]) -> pd.DataFrame:
        """
        Normaliza features numéricas.
        
        Args:
            df: DataFrame com features
            feature_columns: Lista de colunas para normalizar
            
        Returns:
            DataFrame com features normalizadas
        """
        if not self.config.normalize_features or not feature_columns:
            return df
        
        df_result = df.copy()
        
        # Filtra apenas colunas numéricas existentes
        numeric_features = [col for col in feature_columns if col in df_result.columns and 
                          df_result[col].dtype in ['int64', 'float64']]
        
        if numeric_features and self.scaler:
            # Normaliza as features
            df_result[numeric_features] = self.scaler.fit_transform(df_result[numeric_features])
        
        return df_result
    
    def get_feature_importance_ranking(self, df: pd.DataFrame, 
                                     target_column: str = 'sales_amount_sum') -> pd.DataFrame:
        """
        Calcula ranking de importância das features baseado na correlação.
        
        Args:
            df: DataFrame com features
            target_column: Coluna alvo para análise
            
        Returns:
            DataFrame com ranking de importância
        """
        if df.empty or target_column not in df.columns:
            return pd.DataFrame()
        
        # Seleciona apenas colunas numéricas
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if target_column not in numeric_columns:
            return pd.DataFrame()
        
        # Calcula correlações
        correlations = df[numeric_columns].corr()[target_column].abs().sort_values(ascending=False)
        
        # Cria DataFrame de ranking
        ranking_df = pd.DataFrame({
            'feature': correlations.index,
            'correlation': correlations.values,
            'importance_rank': range(1, len(correlations) + 1)
        })
        
        return ranking_df
