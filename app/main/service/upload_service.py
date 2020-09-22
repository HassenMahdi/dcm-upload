import threading
import traceback
from datetime import time

from eventlet.green import thread
from flask import copy_current_request_context
from pymongo import InsertOne

from app.db.Models.domain_collection import DomainCollection
from app.db.Models.flow_context import FlowContext, STATUS
from app.db.Models.modifications import Modifications
from app.engine.engines import EngineFactory
from app.main import db
from app.main.dto.paginator import Paginator
from app.main.service.datafactory_service import stage_factory_upload
from app.main.util.tools import divide_chunks


def stage_upload(upload_context):
    flow_id = upload_context.get("id",None)
    flow = FlowContext(**dict(id = flow_id)).load()

    # ======IF STARTED RETURN THE ID ====== #
    if not flow.not_started():
        return flow.id

    # ============= START UPLOAD THREAD ===================#
    # SET UP UPLOAD META DATA
    flow.setup_metadata(upload_context)
    flow.set_as_started(**upload_context).save()

    try:
        # START THREAD CONTEXTUAL
        @copy_current_request_context
        def ctx_bridge():
            start_upload(flow)
        thr = threading.Thread(target=ctx_bridge)
        thr.start()
        # THREAD END
    except:
        traceback.print_stack()
        flow.set_as_error(traceback.format_exception()).save()

    return flow.id


def start_upload(flow: FlowContext):
    try:
        flow_id = flow.id
        flow.set_as_running().save()
        df = EngineFactory.get_engine(flow)
        filepath = flow.filepath
        df.read_csv(filepath)

        # APPLY MODIFICATIONS
        modifications = Modifications(**dict(worksheet_id=flow.worksheet)).load()
        modifications.apply_modifications(df.frame)
        flow.set_status("APPLIED_MODIFICATIONS").save()

        # SET UP META DATA
        total_records = len(df.frame)
        columns = df.columns
        flow.set_upload_meta(total_records, columns).set_status("LOADED_DATAFRAME").save()
        df['flow_id']=flow.id

        # TODO FORM GENERATOR YIELD IN CHUNKS
        # for chunk in divide_chunks(df.frame, 10):
        dict_gen = df.to_dict_generator()
        # DEBUG SLEEP 10 seconds
        # OPEN TRANSACTION MODE
        flow.set_status("STARTING_BULK_INSERT").save()
        with DomainCollection.start_session() as session:
            # TODO CHUNK ITNO THREADS
            try:
                session.start_transaction()
                ops_gen = [InsertOne(line) for line in dict_gen]
                DomainCollection.bulk_ops(ops_gen, domain_id=flow.domain_id, session=session)
                flow.append_inserted_and_save(len(ops_gen))
            except Exception as bulk_exception:
                session.abort_transaction()
                raise bulk_exception
            finally:
                session.end_session()

        # TODO UPLOAD FILE IN AZURE CONTAINER TO TRIGGER DATA FACTORY
        stage_factory_upload(flow.domain_id, flow.id)

        flow.set_as_done().save()
    except Exception as bwe:
        traceback.print_stack()
        flow.set_as_error(str(bwe)).save()


def get_upload_status(flow_id):
    return FlowContext(**dict(id=flow_id)).load()


def save_flow_context(upload_context: dict):
    fc = FlowContext(**dict(id=upload_context.pop('id', None))).load()
    fc.setup_metadata(upload_context)
    return fc.save()


def get_all_flow_contexts(domain_id,sort_key,sort_acn,page,size):
    # SORT
    sort_key = sort_key or 'upload_start_date'
    sort_acn = sort_acn or -1

    # PAGINATION
    page = page or 1
    size = size or 15
    skip = (page - 1) * size

    query= {}
    if domain_id:
        query.update(dict(domain_id=domain_id))
    collection = FlowContext().db()
    # INDEX COL FOR SORT WORKAROUND
    collection.create_index([(sort_key,1)])

    cursor = collection.find(query)
    cursor = cursor.sort([(sort_key, sort_acn)])

    # PAGINATION
    total = cursor.count()
    cursor = cursor.skip(skip).limit(size)
    data = [FlowContext(**entity) for entity in cursor]
    return Paginator(data, page, size, total)