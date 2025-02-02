#!/usr/bin/env python
"""
Main entry point for the BCG Case Study analysis pipeline.
To execute: bash command 
spark-submit run_analysis.py --config config/config.yml
"""
import argparse
import os
from pyspark.sql import SparkSession
from src.config_manager import ConfigManager
from src.data_loader import DataLoader
from src.analysis import AnalysisRunner
from src.logger import configure_logger

def main() -> None:
    """Orchestrate the analysis pipeline:
    - Parse CLI arguments
    - Initialize Spark and configurations
    - Load data and execute analyses
    """
    
    # Initial console logger for bootstrap process
    logger = configure_logger(__name__, "INFO")
    
    try:
        # Parse command-line arguments
        parser = argparse.ArgumentParser(description='BCG Case Study')
        parser.add_argument('--config', default='config/config.yml', 
                         help='Path to config file')
        args = parser.parse_args()

        # Validate config file exists
        if not os.path.exists(args.config):
            raise FileNotFoundError(f"Config file not found at {args.config}")

        # Load configuration
        config = ConfigManager(args.config)
        
        # Reconfigure logger with config settings
        log_level = config.get_log_level()
        logger = configure_logger(__name__, log_level)
        logger.info("Starting analysis pipeline")

        # Initialize Spark
        spark = SparkSession.builder \
            .master("local")\
            .appName('BCG Case Study') \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        # Load data
        data_config = config.get_data_config()
        data_loader = DataLoader(spark, data_config['input_path'])
        data = {
            'primary_person': data_loader.load_csv(data_config['primary_person']),
            'units': data_loader.load_csv(data_config['units']),
            'charges': data_loader.load_csv(data_config['charges']),
            'damages': data_loader.load_csv(data_config['damages'])
        }

        # Execute analyses
        analysis_runner = AnalysisRunner(spark, data, data_config['output_path'])
        for analysis in config.get_analyses_config():
            logger.info(f"Starting {analysis}...")
            result_df = analysis_runner.run_analysis(analysis)
            result_df.count()  # Trigger action
            print(f"\n{'='*40}")
            print(f"COMPLETED {analysis.upper().replace('_', ' ')}")
            print(f"{'='*40}")
            logger.info(f"Completed {analysis} with {result_df.count()} records")

    except Exception as e:
        logger.error(f"Application failed: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()