"""
Orquestrador principal do pipeline de matriz de merecimento.
"""

import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging
from datetime import datetime, timedelta

from domain.data_loader import DomainDataLoader, DomainDataConfig
from processed.data_processor import DataProcessor, ProcessingConfig
from feature.feature_engineer import FeatureEngineer, FeatureConfig


@dataclass
class PipelineConfig:
    """Configuração geral do pipeline."""
    
    # Configurações de datas
    start_date: str
    end_date: str
    
    # Configurações de dados
    group_columns: List[str] = None
    
    # Configurações de processamento
    outlier_threshold: float = 3.0
    min_sales_threshold: int = 1
    max_sales_threshold: int = 10000
    
    # Configurações de features
    lookback_periods: List[int] = None
    normalize_features: bool = True
    
    def __post_init__(self):
        if self.group_columns is None:
            self.group_columns = ['sku', 'store_id']
        if self.lookback_periods is None:
            self.lookback_periods = [1, 3, 6, 12]


class PipelineOrchestrator:
    """Orquestrador principal do pipeline."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.setup_logging()
        
        # Inicializa componentes do pipeline
        self.domain_config = DomainDataConfig(
            sales_data_path="data/raw/sales.csv",
            products_catalog_path="data/raw/products.csv",
            stores_data_path="data/raw/stores.csv",
            need_groups_path="data/raw/need_groups.csv"
        )
        
        self.processing_config = ProcessingConfig(
            outlier_threshold=config.outlier_threshold,
            min_sales_threshold=config.min_sales_threshold,
            max_sales_threshold=config.max_sales_threshold,
            moving_average_windows=config.lookback_periods
        )
        
        self.feature_config = FeatureConfig(
            lookback_periods=config.lookback_periods,
            normalize_features=config.normalize_features
        )
        
        # Inicializa componentes
        self.data_loader = DomainDataLoader(self.domain_config)
        self.data_processor = DataProcessor(self.processing_config)
        self.feature_engineer = FeatureEngineer(self.feature_config)
        
        # Dados intermediários
        self.raw_data = {}
        self.processed_data = {}
        self.feature_data = {}
    
    def setup_logging(self):
        """Configura sistema de logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'logs/pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def run_pipeline(self) -> Dict[str, pd.DataFrame]:
        """
        Executa o pipeline completo.
        
        Returns:
            Dicionário com dados de cada camada
        """
        self.logger.info("Iniciando execução do pipeline de matriz de merecimento")
        
        try:
            # Etapa 1: Carregamento de dados brutos (Domain)
            self.logger.info("Etapa 1: Carregando dados brutos...")
            self._load_raw_data()
            
            # Etapa 2: Processamento e limpeza (Processed)
            self.logger.info("Etapa 2: Processando e limpando dados...")
            self._process_data()
            
            # Etapa 3: Engenharia de features (Feature)
            self.logger.info("Etapa 3: Criando features...")
            self._create_features()
            
            # Etapa 4: Validação e qualidade
            self.logger.info("Etapa 4: Validando qualidade dos dados...")
            self._validate_pipeline_output()
            
            self.logger.info("Pipeline executado com sucesso!")
            
            return {
                'raw_data': self.raw_data,
                'processed_data': self.processed_data,
                'feature_data': self.feature_data
            }
            
        except Exception as e:
            self.logger.error(f"Erro durante execução do pipeline: {str(e)}")
            raise
    
    def _load_raw_data(self):
        """Carrega dados brutos da camada domain."""
        self.raw_data = {
            'sales': self.data_loader.load_sales_data(
                self.config.start_date, 
                self.config.end_date
            ),
            'products': self.data_loader.load_products_catalog(),
            'stores': self.data_loader.load_stores_data(),
            'need_groups': self.data_loader.load_need_groups()
        }
        
        # Valida qualidade dos dados carregados
        for data_type, df in self.raw_data.items():
            quality_metrics = self.data_loader.validate_data_quality(df, data_type)
            self.logger.info(f"Qualidade dos dados {data_type}: {quality_metrics}")
    
    def _process_data(self):
        """Processa dados na camada processed."""
        if self.raw_data['sales'].empty:
            self.logger.warning("Dados de vendas vazios, pulando processamento")
            return
        
        # Limpa dados de vendas
        clean_sales = self.data_processor.clean_sales_data(self.raw_data['sales'])
        
        # Agrega por período
        aggregated_sales = self.data_processor.aggregate_sales_by_period(
            clean_sales, 
            self.config.group_columns
        )
        
        # Calcula médias móveis
        sales_with_ma = self.data_processor.calculate_moving_averages(
            aggregated_sales,
            'sales_amount_sum',
            self.config.group_columns
        )
        
        # Filtra períodos anormais
        final_sales = self.data_processor.filter_abnormal_periods(
            sales_with_ma,
            'sales_amount_sum',
            self.config.group_columns
        )
        
        self.processed_data = {
            'clean_sales': clean_sales,
            'aggregated_sales': aggregated_sales,
            'sales_with_ma': sales_with_ma,
            'final_sales': final_sales
        }
        
        self.logger.info(f"Dados processados: {len(final_sales)} registros finais")
    
    def _create_features(self):
        """Cria features na camada feature."""
        if not self.processed_data or 'final_sales' not in self.processed_data:
            self.logger.warning("Dados processados não disponíveis, pulando criação de features")
            return
        
        final_sales = self.processed_data['final_sales']
        
        # Features temporais
        temporal_features = self.feature_engineer.create_time_based_features(
            final_sales,
            group_columns=self.config.group_columns
        )
        
        # Features de demanda
        demand_features = self.feature_engineer.create_demand_features(
            temporal_features,
            group_columns=self.config.group_columns
        )
        
        # Features de risco
        risk_features = self.feature_engineer.create_risk_features(
            demand_features,
            group_columns=self.config.group_columns
        )
        
        # Features de loja (se dados disponíveis)
        if not self.raw_data['stores'].empty:
            store_features = self.feature_engineer.create_store_features(
                risk_features,
                self.raw_data['stores'],
                group_columns=self.config.group_columns
            )
        else:
            store_features = risk_features
        
        # Features de produto (se dados disponíveis)
        if not self.raw_data['products'].empty:
            final_features = self.feature_engineer.create_product_features(
                store_features,
                self.raw_data['products'],
                group_columns=self.config.group_columns
            )
        else:
            final_features = store_features
        
        # Normaliza features se configurado
        if self.config.normalize_features:
            numeric_columns = final_features.select_dtypes(include=['number']).columns.tolist()
            final_features = self.feature_engineer.normalize_features(
                final_features, 
                numeric_columns
            )
        
        self.feature_data = {
            'temporal_features': temporal_features,
            'demand_features': demand_features,
            'risk_features': risk_features,
            'store_features': store_features if 'stores' in self.raw_data else None,
            'product_features': final_features if 'products' in self.raw_data else None,
            'final_features': final_features
        }
        
        self.logger.info(f"Features criadas: {len(final_features)} registros com {len(final_features.columns)} colunas")
    
    def _validate_pipeline_output(self):
        """Valida qualidade da saída do pipeline."""
        if not self.feature_data or 'final_features' not in self.feature_data:
            self.logger.warning("Features finais não disponíveis para validação")
            return
        
        final_features = self.feature_data['final_features']
        
        # Validações básicas
        validation_results = {
            'total_records': len(final_features),
            'total_features': len(final_features.columns),
            'null_percentage': (final_features.isnull().sum().sum() / (len(final_features) * len(final_features.columns))) * 100,
            'duplicate_records': final_features.duplicated().sum(),
            'feature_types': final_features.dtypes.value_counts().to_dict()
        }
        
        self.logger.info(f"Validação do pipeline: {validation_results}")
        
        # Gera ranking de importância das features
        if 'sales_amount_sum' in final_features.columns:
            feature_ranking = self.feature_engineer.get_feature_importance_ranking(
                final_features, 
                'sales_amount_sum'
            )
            self.logger.info(f"Top 10 features por importância:\n{feature_ranking.head(10)}")
    
    def get_pipeline_summary(self) -> Dict[str, any]:
        """
        Retorna resumo do pipeline executado.
        
        Returns:
            Dicionário com resumo das execuções
        """
        summary = {
            'execution_timestamp': datetime.now().isoformat(),
            'config': {
                'start_date': self.config.start_date,
                'end_date': self.config.end_date,
                'group_columns': self.config.group_columns
            },
            'data_volumes': {
                'raw_sales': len(self.raw_data.get('sales', pd.DataFrame())),
                'processed_sales': len(self.processed_data.get('final_sales', pd.DataFrame())),
                'final_features': len(self.feature_data.get('final_features', pd.DataFrame()))
            },
            'status': 'completed' if self.feature_data else 'failed'
        }
        
        return summary


def main():
    """Função principal para execução do pipeline."""
    # Configuração de exemplo
    config = PipelineConfig(
        start_date="2023-01-01",
        end_date="2023-12-31",
        group_columns=['sku', 'store_id'],
        normalize_features=True
    )
    
    # Executa pipeline
    orchestrator = PipelineOrchestrator(config)
    results = orchestrator.run_pipeline()
    
    # Exibe resumo
    summary = orchestrator.get_pipeline_summary()
    print(f"Resumo do pipeline: {summary}")


if __name__ == "__main__":
    main()
