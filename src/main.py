#!/usr/bin/env python3
"""
Script principal para execução do pipeline de matriz de merecimento.
"""

import argparse
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Adiciona o diretório src ao path
sys.path.append(str(Path(__file__).parent))

from pipeline_orchestrator import PipelineOrchestrator, PipelineConfig
from config import get_config


def setup_logging(log_level: str = "INFO"):
    """Configura sistema de logging."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def parse_arguments():
    """Parse argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        description="Pipeline de matriz de merecimento para abastecimento de lojas"
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        default=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        help="Data de início para análise (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Data de fim para análise (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--group-columns",
        nargs="+",
        default=["sku", "store_id"],
        help="Colunas para agrupamento (padrão: sku store_id)"
    )
    
    parser.add_argument(
        "--outlier-threshold",
        type=float,
        default=3.0,
        help="Threshold para detecção de outliers (padrão: 3.0)"
    )
    
    parser.add_argument(
        "--min-sales-threshold",
        type=int,
        default=1,
        help="Threshold mínimo de vendas (padrão: 1)"
    )
    
    parser.add_argument(
        "--max-sales-threshold",
        type=int,
        default=10000,
        help="Threshold máximo de vendas (padrão: 10000)"
    )
    
    parser.add_argument(
        "--lookback-periods",
        nargs="+",
        type=int,
        default=[1, 3, 6, 12],
        help="Períodos para lookback em meses (padrão: 1 3 6 12)"
    )
    
    parser.add_argument(
        "--normalize-features",
        action="store_true",
        default=True,
        help="Normalizar features (padrão: True)"
    )
    
    parser.add_argument(
        "--no-normalize-features",
        dest="normalize_features",
        action="store_false",
        help="Não normalizar features"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nível de logging (padrão: INFO)"
    )
    
    parser.add_argument(
        "--config-file",
        type=str,
        help="Arquivo de configuração personalizado"
    )
    
    return parser.parse_args()


def main():
    """Função principal."""
    # Parse argumentos
    args = parse_arguments()
    
    # Configura logging
    logger = setup_logging(args.log_level)
    logger.info("Iniciando pipeline de matriz de merecimento")
    
    try:
        # Cria configuração do pipeline
        config = PipelineConfig(
            start_date=args.start_date,
            end_date=args.end_date,
            group_columns=args.group_columns,
            outlier_threshold=args.outlier_threshold,
            min_sales_threshold=args.min_sales_threshold,
            max_sales_threshold=args.max_sales_threshold,
            lookback_periods=args.lookback_periods,
            normalize_features=args.normalize_features
        )
        
        logger.info(f"Configuração do pipeline:")
        logger.info(f"  Período: {args.start_date} a {args.end_date}")
        logger.info(f"  Agrupamento: {args.group_columns}")
        logger.info(f"  Threshold de outliers: {args.outlier_threshold}")
        logger.info(f"  Threshold de vendas: {args.min_sales_threshold} - {args.max_sales_threshold}")
        logger.info(f"  Períodos de lookback: {args.lookback_periods}")
        logger.info(f"  Normalizar features: {args.normalize_features}")
        
        # Cria e executa o pipeline
        orchestrator = PipelineOrchestrator(config)
        results = orchestrator.run_pipeline()
        
        # Exibe resumo
        summary = orchestrator.get_pipeline_summary()
        logger.info("Pipeline executado com sucesso!")
        logger.info(f"Resumo: {summary}")
        
        # Salva resultados se configurado
        if config.save_intermediate_results:
            logger.info("Salvando resultados intermediários...")
            # TODO: Implementar salvamento de resultados
        
        return 0
        
    except Exception as e:
        logger.error(f"Erro durante execução do pipeline: {str(e)}")
        logger.exception("Detalhes do erro:")
        return 1


if __name__ == "__main__":
    sys.exit(main())
