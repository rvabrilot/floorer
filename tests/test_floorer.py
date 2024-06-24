import os
import pandas as pd
from floorer import Floorer

def test_floorer():
    # Initialize Floorer with a test directory
    test_dir = 'test_parquet_files'
    floorer = Floorer(test_dir)

    # Create sample DataFrame
    df = pd.DataFrame({'name': ['Alice'], 'age': [25]})
    df.name = 'test_people'

    # Test create_parquet
    floorer.create_parquet(df)
    assert os.path.exists(os.path.join(test_dir, 'test_people.parquet'))

    # Test read_parquet
    read_df = floorer.read_parquet('test_people')
    assert read_df.equals(df)

    # Test update_parquet
    df_updated = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [25, 30]})
    df_updated.name = 'test_people'
    floorer.update_parquet(df_updated)
    read_updated_df = floorer.read_parquet('test_people')
    assert read_updated_df.equals(df_updated)

    # Test delete_parquet
    floorer.delete_parquet('test_people')
    assert not os.path.exists(os.path.join(test_dir, 'test_people.parquet'))

    # Clean up test directory
    if os.path.exists(test_dir):
        for f in os.listdir(test_dir):
            os.remove(os.path.join(test_dir, f))
        os.rmdir(test_dir)
