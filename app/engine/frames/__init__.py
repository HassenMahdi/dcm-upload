from app.engine import DataFrameEngine
from app.engine.frames.pandas import PandasEngine


class EngineFactory:

    engines = [PandasEngine()]

    @classmethod
    def get_engine(cls, context) -> DataFrameEngine:
        # TODO GET ENGINES BASED ON CONTEXT
        return cls.engines[0]
