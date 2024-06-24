# Floorer

`Floorer` is a simple Python library for managing pandas DataFrames in Parquet files using PyArrow. 
It provides CRUD operations and maintains a catalog of all DataFrames.

## Installation

```bash
pip install floorer
```

## Usage
```python
import pandas as pd
from floorer import Floorer

# Initialize Floorer with a directory
floorer = Floorer('parquet_files')

# Create sample DataFrames
df1 = pd.DataFrame({
    'name': ['Alice', 'Bob'],
    'age': [25, 30]
})
df1.name = 'people'

df2 = pd.DataFrame({
    'product': ['A', 'B'],
    'price': [100, 200]
})
df2.name = 'products'

# Create Parquet files
floorer.create_parquet(df1)
floorer.create_parquet(df2)

# Read a Parquet file
read_df = floorer.read_parquet('people')

# Update a Parquet file
df1_updated = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})
df1_updated.name = 'people'
floorer.update_parquet(df1_updated)

# Delete a Parquet file
floorer.delete_parquet('products')

# Get the catalog
catalog_df = floorer.get_catalog()
```