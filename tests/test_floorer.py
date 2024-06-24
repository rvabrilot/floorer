import os
import pandas as pd
from floorer import Floorer

def test_floorer():
    # Initialize Floorer with a test directory
    test_dir = os.path.join(os.getcwd(),'tests','parquet_files')
    os.makedirs(test_dir, exist_ok=True)
    floorer = Floorer(test_dir)

    # Create sample DataFrame
    df = pd.DataFrame({'name': ['Alice'], 'age': [25]})
    name = 'test_people'

    # Test create_parquet
    floorer.create_parquet(df, name)
    parquet_path = os.path.join(test_dir, f'{name}.parquet')
    assert os.path.exists(parquet_path), f"Parquet file not found: {parquet_path}"

    # Test read_parquet
    read_df = floorer.read_parquet(name)
    assert read_df.equals(df), "Read DataFrame does not match the original DataFrame"

    # Test update_parquet
    df_updated = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [25, 30]})
    floorer.update_parquet(df_updated, name)
    read_updated_df = floorer.read_parquet(name)
    assert read_updated_df.equals(df_updated), "Updated DataFrame does not match"

    # Test delete_parquet
    floorer.delete_parquet(name)
    assert not os.path.exists(parquet_path), f"Parquet file still exists: {parquet_path}"

    # Clean up test directory
    if os.path.exists(test_dir):
        for f in os.listdir(test_dir):
            os.remove(os.path.join(test_dir, f))
        os.rmdir(test_dir)

# Run the test
test_floorer()
