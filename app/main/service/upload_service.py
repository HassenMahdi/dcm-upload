import threading
import traceback

from eventlet.green import thread
from flask import copy_current_request_context
from pymongo import InsertOne

from app.db.Models.domain_collection import DomainCollection
from app.db.Models.flow_context import FlowContext, STATUS
from app.engine.engines import EngineFactory
from app.main import db
from app.main.util.tools import divide_chunks


def pend_upload(upload_context):

    flow = FlowContext(**upload_context).load()

    # ======IF STARTED RETURN THE ID ====== #
    if not flow.not_started():
        return flow.id

    # ============= START UPLOAD THREAD ===================#
    # SET UP UPLOAD META DATA
    flow.set_as_started(**upload_context).save()

    # START THREAD CONTEXTUAL
    @copy_current_request_context
    def ctx_bridge():
        upload(flow)
    thr = threading.Thread(target=ctx_bridge)
    thr.start()

    return flow.id


def upload(flow: FlowContext):
    try:
        flow.set_as_running().save()
        df = EngineFactory.get_engine(flow)
        filepath = flow.filepath
        df.read_csv(filepath)

        # SET UP META DATA
        total_records = len(df.frame)
        flow.set_upload_meta(total_records).save()

        df['flow_id']=flow.id

        # TODO FORM GENERATOR YIELD IN CHUNKS
        for chunk in divide_chunks(df.frame, 10):
            dict_gen = df.to_dict_generator(chunk)
            ops_gen = [InsertOne(line) for line in dict_gen]
            DomainCollection.bulk_ops(ops_gen, domain_id = flow.domain_id)
            flow.append_inserted_and_save(len(ops_gen))

        # TODO UPLOAD FILE IN AZURE CONTAINER TO TRIGGER DATA FACTORY
        flow.set_as_done().save()
    except Exception as bwe:
        traceback.print_stack()
        flow.set_as_error(str(bwe)).save()


def get_upload_status(flow_id):
    return FlowContext(**dict(id=flow_id)).load()


def save_flow_context(upload_context: dict):
    flow_id = upload_context.pop('id') if id in upload_context else None
    fc = FlowContext(**dict(id=flow_id)).load()

    for k, v in upload_context.items():
        fc.__setattr__(k, v)

    return fc.save()


def get_all_flow_contexts(domain_id = None):
    # return FlowContext.get_all(dict(domain_id=domain_id))
    return FlowContext.get_all()