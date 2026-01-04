import pandas as pd
import numpy as np
from datetime import datetime
import os

# Create sample data for testing
def create_sample_data():
    """Create sample CSV data for testing the pipeline"""
    
    # Generate sample customer data
    np.random.seed(42)
    n_customers = 1000
    
    data = {
        'id': range(1, n_customers + 1),
        'name': [f'Customer_{i}' for i in range(1, n_customers + 1)],
        'email': [f'customer{i}@example.com' for i in range(1, n_customers + 1)],
        'age': np.random.randint(18, 80, n_customers),
        'salary': np.random.normal(50000, 15000, n_customers),
        'status': np.random.choice(['active', 'inactive', 'pending'], n_customers, p=[0.7, 0.2, 0.1]),
        'created_at': pd.date_range('2023-01-01', periods=n_customers, freq='H'),
        'updated_at': pd.date_range('2023-12-01', periods=n_customers, freq='H')
    }
    
    df = pd.DataFrame(data)
    
    # Add some missing values for testing
    df.loc[df.sample(frac=0.05).index, 'salary'] = np.nan
    df.loc[df.sample(frac=0.02).index, 'age'] = np.nan
    
    # Add some duplicates for testing
    duplicates = df.sample(n=20)
    df = pd.concat([df, duplicates], ignore_index=True)
    
    return df

def main():
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Generate and save sample data
    print("Creating sample data...")
    df = create_sample_data()
    
    # Save to CSV
    csv_path = 'data/source_data.csv'
    df.to_csv(csv_path, index=False)
    print(f"Sample data saved to: {csv_path}")
    print(f"Data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Display sample
    print("\nSample data:")
    print(df.head())
    
    # Data info
    print("\nData info:")
    print(df.info())
    
    # Create additional test files
    print("\nCreating additional test files...")
    
    # Small test dataset
    small_df = df.head(100)
    small_df.to_csv('data/small_test_data.csv', index=False)
    
    # JSON format for API testing
    df.head(50).to_json('data/sample_api_data.json', orient='records', date_format='iso')
    
    print("Test files created successfully!")

if __name__ == "__main__":
    main()