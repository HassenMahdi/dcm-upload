import threading
import traceback

from flask import copy_current_request_context

from app.db.Models.field import TargetField
from app.db.Models.flow_context import FlowContext
from app.db.Models.modifications import Modifications
from app.engine.frames import EngineFactory
from app.engine.sinks.parquet_sink import ParquetSinkEngine
from app.main.dto.paginator import Paginator
from app.main.service.aws_service import main_s3
from app.main.service.azure_service import parquet_to_sql
from app.main.service.upload_mysql_service import sink_to_mysql
from app.main.service.user_service import get_a_user


def stage_upload(upload_context):
    flow_id = upload_context.get("id", None)
    flow = FlowContext(**dict(id=flow_id)).load()
    user_id = upload_context.get('user_id')

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
            start_upload(flow, user_id)

        thr = threading.Thread(target=ctx_bridge)
        thr.start()
        # THREAD END
    except:
        traceback.print_stack()
        flow.set_as_error(traceback.format_exception()).save()

    return flow.id


def start_upload(flow: FlowContext, user_id):
    try:
        flow_id = flow.id
        flow.set_as_running().save()
        df = EngineFactory.get_engine(flow)
        filepath = flow.filepath
        df.read_csv(filepath)

        # APPLY MODIFICATIONS
        Modifications.apply_modifications(df.frame, flow)
        TargetField.apply_types(df.frame, flow)
        flow.set_status("APPLIED_MODIFICATIONS").save()

        # SET UP META DATA
        total_records = len(df.frame)
        columns = df.columns
        flow.set_upload_meta(total_records, columns).set_status("LOADED_DATAFRAME").save()
        df['flow_id'] = flow.id

        # DEBUG SLEEP 10 seconds
        # OPEN TRANSACTION MODE
        flow.set_status("STARTING_BULK_INSERT").save()

        # TODO GET ENGINE AND UPLAOD
        engine = ParquetSinkEngine(flow)

        # Uncomment if feature needed (adf)
        # engine.upload(df)

        flow.append_inserted_and_save(total_records)

        bdd = get_a_user(user_id).userDb
        sink_to_mysql(df.frame, flow.domain_id, bdd)
        main_s3(df=df.frame, filepathcsv=filepath)

        # Uncomment if feature needed (adf)
        # if flow.get_enable_df:
        #     adf_run_id = parquet_to_sql(flow)
        #     flow.set_adf_as_started(adf_run_id)

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


def get_all_flow_contexts(domain_id, sort_key, sort_acn, page, size):
    # SORT
    sort_key = sort_key or 'upload_start_date'
    sort_acn = sort_acn or -1

    # PAGINATION
    page = page or 1
    size = size or 15
    skip = (page - 1) * size

    query = {}
    if domain_id:
        query.update(dict(domain_id=domain_id))
    collection = FlowContext().db()

    # INDEX COL FOR SORT WORKAROUND
    # collection.create_index([(sort_key,1)])
    user_lookup = {
        "from": 'users',
        "localField": 'user_id',
        "foreignField": '_id',
        "as": 'user'
    }

    cursor = collection.aggregate([
        {'$match': query},
        {'$sort': {sort_key: sort_acn}},
        {'$skip': skip},
        {'$limit': size},
        {'$lookup': user_lookup},
        {"$unwind": {"preserveNullAndEmptyArrays": True, 'path': '$user'}},
    ])

    data = [FlowContext(**entity).set_user(entity.get('user', None)) for entity in cursor]

    total = collection.find(query, {'_id': 1}).count()

    return Paginator(data, page, size, total)
