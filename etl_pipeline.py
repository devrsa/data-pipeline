import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from typing import Dict, List, Optional
import json
from datetime import datetime, timedelta
from loguru import logger
import os
from dotenv import load_dotenv

load_dotenv()

class ETLPipeline:
    def __init__(self, config_path: str = "config/etl_config.json"):
        self.config = self._load_config(config_path)
        self.engine = self._create_db_connection()
        logger.info("ETL Pipeline initialized")
    
    def _load_config(self, config_path: str) -> Dict:
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        return {
            "database": {
                "url": os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/datawarehouse"),
                "schema": "public"
            },
            "sources": {
                "csv_path": "data/source_data.csv",
                "api_endpoint": "https://api.example.com/data"
            },
            "transformations": {
                "remove_duplicates": True,
                "fill_missing": "forward_fill",
                "date_columns": ["created_at", "updated_at"]
            }
        }
    
    def _create_db_connection(self):
        try:
            engine = create_engine(self.config["database"]["url"])
            logger.info("Database connection established")
            return engine
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def extract_csv(self, file_path: str) -> pd.DataFrame:
        try:
            logger.info(f"Extracting data from CSV: {file_path}")
            df = pd.read_csv(file_path)
            logger.info(f"Extracted {len(df)} rows from CSV")
            return df
        except Exception as e:
            logger.error(f"Failed to extract CSV: {e}")
            raise
    
    def extract_api(self, endpoint: str, params: Optional[Dict] = None) -> pd.DataFrame:
        try:
            import requests
            logger.info(f"Extracting data from API: {endpoint}")
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} rows from API")
            return df
        except Exception as e:
            logger.error(f"Failed to extract from API: {e}")
            raise
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting data transformation")
        
        # Remove duplicates
        if self.config["transformations"]["remove_duplicates"]:
            initial_count = len(df)
            df = df.drop_duplicates()
            logger.info(f"Removed {initial_count - len(df)} duplicate rows")
        
        # Handle missing values
        fill_method = self.config["transformations"]["fill_missing"]
        if fill_method == "forward_fill":
            df = df.fillna(method='ffill')
        elif fill_method == "backward_fill":
            df = df.fillna(method='bfill')
        elif fill_method == "mean":
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
        
        # Convert date columns
        date_columns = self.config["transformations"]["date_columns"]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Add derived columns
        if 'created_at' in df.columns:
            df['processing_date'] = datetime.now()
            df['days_since_creation'] = (datetime.now() - df['created_at']).dt.days
        
        # Data quality checks
        self._validate_data(df)
        
        logger.info(f"Transformation complete. Final shape: {df.shape}")
        return df
    
    def _validate_data(self, df: pd.DataFrame):
        # Basic data quality checks
        null_percentage = (df.isnull().sum() / len(df)) * 100
        high_null_cols = null_percentage[null_percentage > 50].index.tolist()
        
        if high_null_cols:
            logger.warning(f"High null values in columns: {high_null_cols}")
        
        # Check for data consistency
        if 'id' in df.columns:
            duplicate_ids = df['id'].duplicated().sum()
            if duplicate_ids > 0:
                logger.warning(f"Found {duplicate_ids} duplicate IDs")
    
    def load(self, df: pd.DataFrame, table_name: str, if_exists: str = "append"):
        try:
            logger.info(f"Loading {len(df)} rows to table: {table_name}")
            
            # Create table if it doesn't exist
            df.head(0).to_sql(
                name=table_name,
                con=self.engine,
                schema=self.config["database"]["schema"],
                if_exists="replace",
                index=False
            )
            
            # Load data
            rows_affected = df.to_sql(
                name=table_name,
                con=self.engine,
                schema=self.config["database"]["schema"],
                if_exists=if_exists,
                index=False,
                chunksize=1000
            )
            
            logger.info(f"Successfully loaded {rows_affected} rows to {table_name}")
            return rows_affected
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def run_pipeline(self, source_type: str = "csv", source_path: str = None, 
                    target_table: str = "processed_data"):
        try:
            # Extract
            if source_type == "csv":
                df = self.extract_csv(source_path or self.config["sources"]["csv_path"])
            elif source_type == "api":
                df = self.extract_api(self.config["sources"]["api_endpoint"])
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            # Transform
            transformed_df = self.transform(df)
            
            # Load
            rows_loaded = self.load(transformed_df, target_table)
            
            # Generate metadata
            metadata = {
                "pipeline_run_id": datetime.now().isoformat(),
                "source_type": source_type,
                "source_path": source_path,
                "target_table": target_table,
                "rows_extracted": len(df),
                "rows_transformed": len(transformed_df),
                "rows_loaded": rows_loaded,
                "status": "success"
            }
            
            self._save_metadata(metadata)
            logger.info("Pipeline completed successfully")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
    
    def _save_metadata(self, metadata: Dict):
        metadata_file = f"logs/pipeline_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadata saved to {metadata_file}")

if __name__ == "__main__":
    # Example usage
    pipeline = ETLPipeline()
    
    # Run pipeline with CSV data
    result = pipeline.run_pipeline(
        source_type="csv",
        source_path="data/sample_data.csv",
        target_table="customer_data"
    )
    
    print(f"Pipeline result: {result}")