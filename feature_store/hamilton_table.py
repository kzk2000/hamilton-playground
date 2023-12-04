import dataclasses
import importlib
import pkgutil
from types import ModuleType
from typing import List

import pandas as pd
from hamilton import driver

from feature_store import all_nodes


@dataclasses.dataclass
class HamiltonTable:
    schema: str
    table_name: str
    nodes: List[str]
    overwrites = ['ds']

    @staticmethod
    def import_default_node_modules(base_module: ModuleType) -> List[ModuleType]:
        modules = []
        for module_info in pkgutil.iter_modules(base_module.__path__):
            module_name = f"{base_module.__name__}.{module_info.name}"
            module = importlib.import_module(module_name)
            modules.append(module)
        return modules

    def load_data(self):
        all_modules = self.import_default_node_modules(all_nodes)

        dr = driver.Driver({}, *all_modules)
        return dr.execute(self.nodes)

    def upload_to_db(self, df: pd.DataFrame):
        # FixMe: logic to upload to data base here
        print(df.head())
        print(f'Uploading to \'{self.schema}.{self.table_name}\'...done.')

    def run(self):
        df = self.load_data();
        self.upload_to_db(df)
