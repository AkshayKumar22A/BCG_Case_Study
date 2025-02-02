import os
from pyspark.sql import SparkSession, DataFrame

class DataLoader:
    """Handles data loading and basic validation."""
    
    def __init__(self, spark: SparkSession, input_path: str):
        """
        Args:
            spark: Active Spark session
            input_path: Base directory containing CSV files
        Raises:
            FileNotFoundError: If input directory is invalid
        """
        self.spark = spark
        self.input_path = input_path
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input path not found: {input_path}")

    def load_csv(self, file_name: str) -> DataFrame:
        """Loads a CSV file into a Spark DataFrame.
        Args:
            file_name: CSV filename (e.g., 'primary_person.csv')
        Returns:
            Deduplicated DataFrame with header and inferred schema
        Raises:
            FileNotFoundError: If CSV file is missing
        """
        full_path = os.path.join(self.input_path, file_name)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Data file not found: {full_path}")
            
        return (self.spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(full_path)
                .dropDuplicates())