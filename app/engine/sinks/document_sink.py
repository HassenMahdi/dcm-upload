from flask import current_app
from pymongo import InsertOne

from app.db.Models.domain_collection import DomainCollection
from app.db.Models.flow_context import FlowContext
from app.engine import SinkEngine, DataFrameEngine
from pydrill.client import PyDrill

from app.main.util.storage import get_path


class DocumentSinkEngine(SinkEngine):
    __SINK_TYPE__ = 'document'

    context: FlowContext = None

    def __init__(self, context: FlowContext):
        super(DocumentSinkEngine, self).__init__(context)

        drill_server = current_app.config['DRILL_SERVER']
        drill_port = current_app.config['DRILL_PORT']

        self._drill = PyDrill(drill_server, drill_port)
        if not self._drill.is_active():
            raise Exception('COULD NOT CONNECT TO DRILL SERVER ')

    def upload(self, frame: DataFrameEngine):
        dict_gen = frame.to_dict_generator()
        with DomainCollection.start_session() as session:
            # TODO CHUNK INTO THREADS
            try:
                session.start_transaction()
                ops_gen = [InsertOne(line) for line in dict_gen]
                DomainCollection.bulk_ops(ops_gen, domain_id=self.context.domain_id, session=session)
            except Exception as bulk_exception:
                session.abort_transaction()
                raise bulk_exception
            finally:
                session.end_session()




