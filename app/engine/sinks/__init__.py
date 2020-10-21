from app.engine import SinkEngine
from app.engine.sinks.parquet_sink import ParquetSinkEngine


class SinkFactory:

    engines = [ParquetSinkEngine]

    @classmethod
    def get_engine(cls, context) -> SinkEngine:
        # TODO GET ENGINES BASED ON CONTEXT
        return cls.engines[0](context)
