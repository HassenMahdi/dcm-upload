from flask import current_app

from app.db.Models.flow_context import FlowContext
from app.engine import SinkEngine, DataFrameEngine
from pydrill.client import PyDrill

from app.main.util.storage import get_path


class ParquetSinkEngine(SinkEngine):
    __SINK_TYPE__ = 'parquet'
    _drill = None

    context: FlowContext = None

    def __init__(self, context: FlowContext):
        super(ParquetSinkEngine, self).__init__(context)

        # drill_server = current_app.conf['DRILL_SERVER']
        # drill_port = current_app.conf['DRILL_PORT']
        #
        # self._drill = PyDrill(drill_server, drill_port)
        # if not self._drill.is_active():
        #     raise Exception('COULD NOT CONNECT TO DRILL SERVER ')

    def upload(self, frame: DataFrameEngine):
        domain_id = self.context.domain_id
        flow_id = self.context.id
        file_name = f"{domain_id}_{flow_id}"
        file_path = get_path(current_app.conf['DATA_FOLDER'], file_name)
        print(file_path)
        frame.to_parquet(file_path, engine='fastparquet', compression='gzip')



