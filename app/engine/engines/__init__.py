from app.engine import DataFrameEngine
from app.engine.engines.pandas import PandasEngine


class EngineFactory:

    engines = [PandasEngine()]

    @classmethod
    def get_engine(cls, context) -> DataFrameEngine:
        # TODO GET ENGINES BASED ON CONTEXT
        return cls.engines[0]
