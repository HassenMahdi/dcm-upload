import os

from flask import current_app

from app.db.Models.flow_context import FlowContext
from app.engine import SinkEngine, DataFrameEngine
from pydrill.client import PyDrill

from app.main.service.datafactory_service import copy_parquet_blob
from app.main.util.storage import get_path


class ParquetSinkEngine(SinkEngine):
    __SINK_TYPE__ = 'parquet'
    _drill = None

    context: FlowContext = None

    def __init__(self, context: FlowContext):
        super(ParquetSinkEngine, self).__init__(context)

        # drill_server = current_app.config['DRILL_SERVER']
        # drill_port = current_app.config['DRILL_PORT']
        #
        # self._drill = PyDrill(drill_server, drill_port)
        # if not self._drill.is_active():
        #     raise Exception('COULD NOT CONNECT TO DRILL SERVER ')

    def upload(self, frame: DataFrameEngine):
        domain_id = self.context.domain_id
        flow_id = self.context.id
        file_name = f"{domain_id}.{flow_id}"
        file_path = get_path(os.path.join(current_app.config['UPLOAD_FOLDER'], 'tmp'), file_name, create = True)
        frame.to_parquet(file_path, engine='fastparquet', compression='gzip')
        try:
            blob_name = f'{domain_id}/{flow_id}'
            copy_parquet_blob(file_path, blob_name)
        finally:
            os.remove(file_path)
        # TODO DELETE TMP FILE





