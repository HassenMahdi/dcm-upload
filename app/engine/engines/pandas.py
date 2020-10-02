import pandas as pd

from app.engine import DataFrameEngine


class PandasEngine(DataFrameEngine):

    engine = pd

    def read_csv(self, filepath, *args, **kwargs):
        self.frame = self.engine.read_csv(filepath, error_bad_lines=False,engine="c", dtype=str, skipinitialspace=True, na_filter=False,delimiter=";")
        return self

    def __repr__(self):
        return self.frame.__repr__()




