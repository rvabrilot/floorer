import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import json

class Floorer:
    def __init__(self, directory: str):
        self.directory = os.path.abspath(directory)
        os.makedirs(self.directory, exist_ok=True)
        self.catalog_path = os.path.join(self.directory, 'catalog.json')
        self.catalog = self._load_catalog()

    def _load_catalog(self) -> dict:
        if os.path.exists(self.catalog_path):
            with open(self.catalog_path, 'r') as f:
                return json.load(f)
        else:
            return {}

    def _save_catalog(self) -> None:
        with open(self.catalog_path, 'w') as f:
            json.dump(self.catalog, f, default=str, indent=4)

    def _update_catalog(self, name: str, operation: str) -> None:
        now = datetime.now()
        if operation == 'create':
            self.catalog[name] = {
                'created': now.isoformat(),
                'last_updated': now.isoformat()
            }
        elif operation == 'update':
            if name in self.catalog:
                self.catalog[name]['last_updated'] = now.isoformat()
            else:
                self.catalog[name] = {
                'created': now.isoformat(),
                'last_updated': now.isoformat()
            }
        elif operation == 'delete':
            if name in self.catalog:
                del self.catalog[name]
        self._save_catalog()

    def create_parquet(self, df: pd.DataFrame, name: str) -> None:
        file_path = os.path.join(self.directory, f"{name}.parquet")
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)
        self._update_catalog(name, 'create')

    def read_parquet(self, name: str) -> pd.DataFrame:
        file_path = os.path.join(self.directory, f"{name}.parquet")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No such file: '{file_path}'")
        return pq.read_table(file_path).to_pandas()

    def update_parquet(self, df: pd.DataFrame, name: str) -> None:
        file_path = os.path.join(self.directory, f"{name}.parquet")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No such file: '{file_path}'")
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path)
        self._update_catalog(name, 'update')

    def delete_parquet(self, name: str) -> None:
        file_path = os.path.join(self.directory, f"{name}.parquet")
        if os.path.exists(file_path):
            os.remove(file_path)
            self._update_catalog(name, 'delete')
        else:
            raise FileNotFoundError(f"No such file: '{file_path}'")

    def get_catalog(self) -> dict:
        return self.catalog
