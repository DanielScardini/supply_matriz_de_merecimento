"""
Configurações centralizadas para o pipeline de matriz de merecimento.
"""

import os
from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class DataPaths:
    """Caminhos para dados e arquivos."""
    
    # Diretórios base
    base_dir: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir: str = os.path.join(base_dir, "data")
    logs_dir: str = os.path.join(base_dir, "logs")
    output_dir: str = os.path.join(base_dir, "output")
    
    # Dados brutos
    raw_data_dir: str = os.path.join(data_dir, "raw")
    sales_file: str = os.path.join(raw_data_dir, "sales.csv")
    products_file: str = os.path.join(raw_data_dir, "products.csv")
    stores_file: str = os.path.join(raw_data_dir, "stores.csv")
    need_groups_file: str = os.path.join(raw_data_dir, "need_groups.csv")
    
    # Dados processados
    processed_data_dir: str = os.path.join(data_dir, "processed")
    
    # Features
    features_dir: str = os.path.join(data_dir, "features")
    
    def create_directories(self):
        """Cria diretórios necessários se não existirem."""
        directories = [
            self.data_dir,
            self.raw_data_dir,
            self.processed_data_dir,
            self.features_dir,
            self.logs_dir,
            self.output_dir
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)


@dataclass
class ProcessingParameters:
    """Parâmetros para processamento de dados."""
    
    # Thresholds para outliers
    outlier_threshold: float = 3.0
    min_sales_threshold: int = 1
    max_sales_threshold: int = 10000
    
    # Janelas para médias móveis
    moving_average_windows: List[int] = None
    
    # Thresholds para filtros
    demand_volatility_threshold: float = 0.5
    stockout_threshold: float = 0.1
    
    def __post_init__(self):
        if self.moving_average_windows is None:
            self.moving_average_windows = [3, 6, 9, 12]


@dataclass
class FeatureParameters:
    """Parâmetros para engenharia de features."""
    
    # Períodos para lookback
    lookback_periods: List[int] = None
    
    # Períodos para previsão
    forecast_periods: int = 30
    
    # Configurações de normalização
    normalize_features: bool = True
    scale_features: bool = True
    
    # Thresholds para features de risco
    rupture_risk_threshold: float = 0.5
    seasonality_threshold: float = 0.3
    
    def __post_init__(self):
        if self.lookback_periods is None:
            self.lookback_periods = [1, 3, 6, 12]


@dataclass
class ModelParameters:
    """Parâmetros para o modelo de merecimento."""
    
    # Configurações de treinamento
    test_size: float = 0.2
    random_state: int = 42
    
    # Configurações de validação
    cv_folds: int = 5
    
    # Configurações de features
    min_features: int = 5
    max_features: int = 50
    
    # Thresholds de performance
    min_r2_score: float = 0.6
    max_mae: float = 1000.0


@dataclass
class LoggingConfig:
    """Configuração para sistema de logging."""
    
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_handler: bool = True
    console_handler: bool = True
    
    # Rotação de logs
    max_bytes: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5


@dataclass
class PipelineConfig:
    """Configuração geral do pipeline."""
    
    # Configurações de dados
    data_paths: DataPaths = None
    processing_params: ProcessingParameters = None
    feature_params: FeatureParameters = None
    model_params: ModelParameters = None
    logging_config: LoggingConfig = None
    
    # Configurações de execução
    batch_size: int = 10000
    max_workers: int = 4
    timeout_seconds: int = 3600
    
    # Configurações de validação
    validate_data_quality: bool = True
    generate_reports: bool = True
    save_intermediate_results: bool = True
    
    def __post_init__(self):
        if self.data_paths is None:
            self.data_paths = DataPaths()
        if self.processing_params is None:
            self.processing_params = ProcessingParameters()
        if self.feature_params is None:
            self.feature_params = FeatureParameters()
        if self.model_params is None:
            self.model_params = ModelParameters()
        if self.logging_config is None:
            self.logging_config = LoggingConfig()
        
        # Cria diretórios necessários
        self.data_paths.create_directories()


# Configuração padrão
DEFAULT_CONFIG = PipelineConfig()


def get_config() -> PipelineConfig:
    """
    Retorna configuração padrão do pipeline.
    
    Returns:
        Configuração padrão
    """
    return DEFAULT_CONFIG


def load_config_from_file(config_path: str) -> PipelineConfig:
    """
    Carrega configuração de arquivo externo.
    
    Args:
        config_path: Caminho para arquivo de configuração
        
    Returns:
        Configuração carregada
    """
    # TODO: Implementar carregamento de arquivo de configuração
    # Por enquanto retorna configuração padrão
    return DEFAULT_CONFIG


def save_config_to_file(config: PipelineConfig, config_path: str):
    """
    Salva configuração em arquivo externo.
    
    Args:
        config: Configuração a ser salva
        config_path: Caminho para arquivo de configuração
    """
    # TODO: Implementar salvamento de configuração em arquivo
    pass
