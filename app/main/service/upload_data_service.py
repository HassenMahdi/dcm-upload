import os
from datetime import datetime
from time import time

from flask import send_file
from app.db.Models.domain_collection import DomainCollection
from app.db.Models.field import TargetField, FlowTagField
from app.db.Models.flow_context import FlowContext
from app.main.dto.paginator import Paginator
from app.main.util.file_generators import generate_xlsx, generate_csv
from app.main.util.mongo import filters_to_query
from app.main.util.storage import get_export_path


def get_collection_total(domain_id, payload={}):
    if payload.get('page', None) != 1:
        return 0
    projection = {f["column"]:1 for f in payload.get('filters', [])}
    cursor = get_collection_cusror(domain_id, payload, projection, as_count = True)
    result = list(cursor)
    if len(result) > 0:
        return result[0].get('total')
    return 0


def get_collection_cusror(domain_id, payload={}, project={}, skip=None, limit=None, as_count=False):
    collection = DomainCollection().db(domain_id=domain_id)
    # collection.create_index([('_id', 1)])
    # FOR FILTERS
    query = filters_to_query(payload.get('filters', []))

    flow_tags_field = "flow_tags"
    tag_lookup = {
        "from": FlowContext.__TABLE__,
        "localField": 'flow_id',
        "foreignField": '_id',
        "as": flow_tags_field
    }

    project = {**project,f"{flow_tags_field}": f"${flow_tags_field}.upload_tags"}

    agg = [
        {"$lookup": tag_lookup},
        {"$unwind": f'${flow_tags_field}'},
        # {"$project": {"_id" : 0}},
        {"$project": project},
        {"$match": query}
    ]

    if as_count:
        agg.append({"$count": "total"})
    if skip:
        agg.append({"$skip": skip})
    if limit:
        agg.append({"$limit": limit})

    cursor = collection.aggregate(agg)

    return cursor


def get_collection_data(domain_id, payload={}):

    page = payload.get('page', None) or 1
    limit = payload.get('size', None) or 15
    skip = (page - 1) * limit
    fields = TargetField.get_all(domain_id=domain_id)

    total = get_collection_total(domain_id, payload)
    cursor = get_collection_cusror(domain_id, payload, {tf.name: 1 for tf in fields}, skip, limit)

    fields.append(FlowTagField)
    headers = [dict(headerName=tf.label, field=tf.name, type=tf.type) for tf in fields]
    data = []
    for row in cursor:
        data.append({f.name: f.format_value(row.get(f.name, None)) for f in fields})

    return Paginator(data, page, limit, total, headers=headers)


def export_collection_data(domain_id, payload={}, file_type='xlsx'):
    headers = TargetField.get_all(domain_id=domain_id)
    cursor = get_collection_cusror(domain_id, payload, project={tf.name: 1 for tf in headers})

    data = [[h.label for h in headers]]
    for row in cursor:
        data.append([str(row.get(h.name, None)) for h in headers])



    file_path = get_export_path(f"export_{domain_id}_{datetime.now().timestamp()}.{file_type}")

    if file_type == 'xlsx':
        generate_xlsx(file_path, data)
    elif file_type == 'csv':
        generate_csv(file_path, data)

    return send_file(file_path)