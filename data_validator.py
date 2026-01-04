import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from great_expectations.dataset import PandasDataset
from loguru import logger
import json
from datetime import datetime

class DataValidator:
    def __init__(self, config_path: str = "config/validation_rules.json"):
        self.config = self._load_validation_rules(config_path)
        self.validation_results = []
        logger.info("Data Validator initialized")
    
    def _load_validation_rules(self, config_path: str) -> Dict:
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Validation config not found, using defaults")
            return self._get_default_rules()
    
    def _get_default_rules(self) -> Dict:
        return {
            "schema_rules": {
                "required_columns": ["id", "name", "email"],
                "column_types": {
                    "id": "int64",
                    "name": "object",
                    "email": "object",
                    "age": "int64"
                }
            },
            "data_quality_rules": {
                "max_null_percentage": 20,
                "min_rows": 100,
                "max_duplicate_percentage": 5,
                "email_format_regex": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            },
            "business_rules": {
                "age_range": {"min": 0, "max": 120},
                "unique_id": True,
                "positive_values": ["age", "salary"]
            }
        }
    
    def validate_schema(self, df: pd.DataFrame) -> Dict:
        logger.info("Starting schema validation")
        results = {
            "validation_type": "schema",
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "errors": [],
            "warnings": []
        }
        
        # Check required columns
        required_cols = self.config["schema_rules"]["required_columns"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            results["passed"] = False
            results["errors"].append(f"Missing required columns: {missing_cols}")
        
        # Check column types
        expected_types = self.config["schema_rules"]["column_types"]
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if actual_type != expected_type:
                    results["warnings"].append(
                        f"Column '{col}' has type '{actual_type}', expected '{expected_type}'"
                    )
        
        logger.info(f"Schema validation completed. Passed: {results['passed']}")
        return results
    
    def validate_data_quality(self, df: pd.DataFrame) -> Dict:
        logger.info("Starting data quality validation")
        results = {
            "validation_type": "data_quality",
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "errors": [],
            "warnings": [],
            "metrics": {}
        }
        
        # Row count check
        min_rows = self.config["data_quality_rules"]["min_rows"]
        actual_rows = len(df)
        results["metrics"]["row_count"] = actual_rows
        
        if actual_rows < min_rows:
            results["passed"] = False
            results["errors"].append(f"Row count {actual_rows} below minimum {min_rows}")
        
        # Null percentage check
        max_null_pct = self.config["data_quality_rules"]["max_null_percentage"]
        null_percentages = (df.isnull().sum() / len(df)) * 100
        high_null_cols = null_percentages[null_percentages > max_null_pct].index.tolist()
        
        results["metrics"]["null_percentages"] = null_percentages.to_dict()
        
        if high_null_cols:
            results["warnings"].append(
                f"High null values in columns: {high_null_cols}"
            )
        
        # Duplicate check
        max_dup_pct = self.config["data_quality_rules"]["max_duplicate_percentage"]
        duplicate_count = df.duplicated().sum()
        duplicate_percentage = (duplicate_count / len(df)) * 100
        results["metrics"]["duplicate_percentage"] = duplicate_percentage
        
        if duplicate_percentage > max_dup_pct:
            results["warnings"].append(
                f"Duplicate percentage {duplicate_percentage:.2f}% exceeds threshold {max_dup_pct}%"
            )
        
        logger.info(f"Data quality validation completed. Passed: {results['passed']}")
        return results
    
    def validate_business_rules(self, df: pd.DataFrame) -> Dict:
        logger.info("Starting business rules validation")
        results = {
            "validation_type": "business_rules",
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "errors": [],
            "warnings": [],
            "metrics": {}
        }
        
        # Age range validation
        if "age" in df.columns:
            age_range = self.config["business_rules"]["age_range"]
            invalid_ages = df[(df["age"] < age_range["min"]) | (df["age"] > age_range["max"])]
            
            if len(invalid_ages) > 0:
                results["warnings"].append(
                    f"Found {len(invalid_ages)} records with invalid age values"
                )
                results["metrics"]["invalid_age_count"] = len(invalid_ages)
        
        # Unique ID validation
        if self.config["business_rules"]["unique_id"] and "id" in df.columns:
            duplicate_ids = df["id"].duplicated().sum()
            if duplicate_ids > 0:
                results["passed"] = False
                results["errors"].append(f"Found {duplicate_ids} duplicate IDs")
                results["metrics"]["duplicate_id_count"] = duplicate_ids
        
        # Positive values validation
        positive_cols = self.config["business_rules"]["positive_values"]
        for col in positive_cols:
            if col in df.columns:
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    results["warnings"].append(
                        f"Found {negative_count} negative values in column '{col}'"
                    )
        
        # Email format validation
        if "email" in df.columns:
            import re
            email_pattern = self.config["data_quality_rules"]["email_format_regex"]
            invalid_emails = df[~df["email"].str.match(email_pattern, na=False)]
            
            if len(invalid_emails) > 0:
                results["warnings"].append(
                    f"Found {len(invalid_emails)} records with invalid email format"
                )
                results["metrics"]["invalid_email_count"] = len(invalid_emails)
        
        logger.info(f"Business rules validation completed. Passed: {results['passed']}")
        return results
    
    def validate_with_great_expectations(self, df: pd.DataFrame) -> Dict:
        logger.info("Starting Great Expectations validation")
        
        # Convert to Great Expectations dataset
        ge_df = PandasDataset(df)
        
        results = {
            "validation_type": "great_expectations",
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "expectations": []
        }
        
        # Define expectations
        expectations = [
            ("expect_table_row_count_to_be_between", {"min_value": 1}),
            ("expect_columns_to_match_ordered_list", {"column_list": df.columns.tolist()}),
            ("expect_column_values_to_not_be_null", {"column": "id"}),
        ]
        
        # Add column-specific expectations
        for col in df.columns:
            if df[col].dtype == "int64":
                expectations.append(
                    ("expect_column_values_to_be_of_type", {"column": col, "type_": "int64"})
                )
            elif df[col].dtype == "object":
                expectations.append(
                    ("expect_column_values_to_be_of_type", {"column": col, "type_": "str"})
                )
        
        # Run expectations
        for expectation_name, kwargs in expectations:
            try:
                result = getattr(ge_df, expectation_name)(**kwargs)
                results["expectations"].append({
                    "expectation": expectation_name,
                    "kwargs": kwargs,
                    "success": result.success,
                    "result": result.result
                })
                
                if not result.success:
                    results["passed"] = False
                    
            except Exception as e:
                logger.warning(f"Expectation {expectation_name} failed: {e}")
                results["expectations"].append({
                    "expectation": expectation_name,
                    "kwargs": kwargs,
                    "success": False,
                    "error": str(e)
                })
        
        logger.info(f"Great Expectations validation completed. Passed: {results['passed']}")
        return results
    
    def run_full_validation(self, df: pd.DataFrame) -> Dict:
        logger.info("Starting full data validation")
        
        validation_results = {
            "validation_run_id": datetime.now().isoformat(),
            "dataset_shape": df.shape,
            "overall_passed": True,
            "validations": []
        }
        
        # Run all validation types
        validations = [
            self.validate_schema(df),
            self.validate_data_quality(df),
            self.validate_business_rules(df),
            self.validate_with_great_expectations(df)
        ]
        
        for validation in validations:
            validation_results["validations"].append(validation)
            if not validation["passed"]:
                validation_results["overall_passed"] = False
        
        # Save validation results
        self._save_validation_results(validation_results)
        
        logger.info(f"Full validation completed. Overall passed: {validation_results['overall_passed']}")
        return validation_results
    
    def _save_validation_results(self, results: Dict):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"logs/validation_results_{timestamp}.json"
        
        import os
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Validation results saved to {filename}")
    
    def generate_data_profile(self, df: pd.DataFrame) -> Dict:
        logger.info("Generating data profile")
        
        profile = {
            "timestamp": datetime.now().isoformat(),
            "shape": df.shape,
            "columns": {},
            "summary": {}
        }
        
        # Column-level profiling
        for col in df.columns:
            col_profile = {
                "dtype": str(df[col].dtype),
                "non_null_count": df[col].count(),
                "null_count": df[col].isnull().sum(),
                "null_percentage": (df[col].isnull().sum() / len(df)) * 100,
                "unique_count": df[col].nunique(),
                "unique_percentage": (df[col].nunique() / len(df)) * 100
            }
            
            # Add statistics for numeric columns
            if df[col].dtype in ['int64', 'float64']:
                col_profile.update({
                    "mean": df[col].mean(),
                    "median": df[col].median(),
                    "std": df[col].std(),
                    "min": df[col].min(),
                    "max": df[col].max(),
                    "quartiles": df[col].quantile([0.25, 0.5, 0.75]).to_dict()
                })
            
            # Add most frequent values for categorical columns
            if df[col].dtype == 'object':
                col_profile["top_values"] = df[col].value_counts().head(10).to_dict()
            
            profile["columns"][col] = col_profile
        
        # Dataset summary
        profile["summary"] = {
            "total_memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
            "duplicate_rows": df.duplicated().sum(),
            "duplicate_percentage": (df.duplicated().sum() / len(df)) * 100
        }
        
        logger.info("Data profile generated successfully")
        return profile

if __name__ == "__main__":
    # Example usage
    validator = DataValidator()
    
    # Create sample data
    sample_data = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com", "eve@example.com"],
        "age": [25, 30, 35, 28, 32]
    })
    
    # Run validation
    results = validator.run_full_validation(sample_data)
    print(f"Validation results: {results['overall_passed']}")
    
    # Generate profile
    profile = validator.generate_data_profile(sample_data)
    print(f"Data profile generated for {profile['shape']} rows")