import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime

class Floorer:
    def __init__(self, directory: str):
        self.directory = directory
        os.makedirs(self.directory, exist_ok=True)
        self.catalog_path = os.path.join(self.directory, 'catalog.parquet')
        self.catalog = self._load_catalog()

    def _load_catalog(self) -> pd.DataFrame:
        if os.path.exists(self.catalog_path):
            return pq.read_table(self.catalog_path).to_pandas()
        else:
            return pd.DataFrame(columns=['name', 'created', 'last_updated'])

    def _save_catalog(self) -> None:
        table = pa.Table.from_pandas(self.catalog)
        pq.write_table(table, self.catalog_path)

    def _update_catalog(self, name: str, operation: str) -> None:
        now = datetime.now()
        if operation == 'create':
            self.catalog = self.catalog.append({
                'name': name,
                'created': now,
                'last_updated': now
            }, ignore_index=True)
        elif operation == 'update':
            self.catalog.loc[self.catalog['name'] == name, 'last_updated'] = now
        elif operation == 'delete':
            self.catalog = self.catalog[self.catalog['name'] != name]
        self._save_catalog()

    def create_parquet(self, df: pd.DataFrame) -> None:
        file_path = os.path.join(self.directory, f"{df.name}.parquet")
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)
        self._update_catalog(df.name, 'create')

    def read_parquet(self, name: str) -> pd.DataFrame:
        file_path = os.path.join(self.directory, f"{name}.parquet")
        return pq.read_table(file_path).to_pandas()

    def update_parquet(self, df: pd.DataFrame) -> None:
        file_path = os.path.join(self.directory, f"{df.name}.parquet")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No such file: '{file_path}'")
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)
        self._update_catalog(df.name, 'update')

    def delete_parquet(self, name: str) -> None:
        file_path = os.path.join(self.directory, f"{name}.parquet")
        if os.path.exists(file_path):
            os.remove(file_path)
            self._update_catalog(name, 'delete')
        else:
            raise FileNotFoundError(f"No such file: '{file_path}'")

    def get_catalog(self) -> pd.DataFrame:
        return self.catalog
