import os
from datetime import datetime
from time import time

from flask import send_file
from app.db.Models.domain_collection import DomainCollection
from app.db.Models.field import TargetField, FlowTagField
from app.db.Models.flow_context import FlowContext
from app.main.dto.paginator import Paginator
from app.main.service.azure_service import download_data_as_table
from app.main.util.file_generators import generate_xlsx, generate_csv
from app.main.util.mongo import filters_to_query
from app.main.util.parquet import iter_parquet
from app.main.util.storage import get_export_path

import pyarrow as pa


def get_paginated_data(domain_id, limit=None, skip=None):
    """Gets the page data"""
    parquet_table = download_data_as_table(domain_id)
    if skip is not None and limit is not None:
        row_indices = range(skip, min(skip + limit, len(parquet_table)))
        table = parquet_table.take(list(row_indices))
    else:
        table = parquet_table

    # TODO
    # ADD UPLOAD TAGES HERE FROM MONGO
    table.append_column('flow_tags',  pa.array(['TEMP TAG'] * len(table), pa.string()))

    return iter_parquet(table), parquet_table.num_rows


def get_collection_data(domain_id, payload={}):

    page = payload.get('page', None) or 1
    limit = payload.get('size', None) or 15
    skip = (page - 1) * limit
    fields = TargetField.get_all(domain_id=domain_id)

    cursor, total = get_paginated_data(domain_id, limit, skip)
    fields.append(FlowTagField)
    headers = [dict(headerName=tf.label, field=tf.name, type=tf.type) for tf in fields]
    data = []
    for row in cursor:
        data.append({f.name: f.format_value(row.get(f.name, None)) for f in fields})

    return Paginator(data, page, limit, total, headers=headers)


def export_collection_data(domain_id, payload={}, file_type='xlsx'):
    headers = TargetField.get_all(domain_id=domain_id)
    cursor, total = get_paginated_data(domain_id, payload.get("page", None), payload.get("size", None))

    data = [[h.label for h in headers]]
    for row in cursor:
        data.append([str(row.get(h.name, None)) for h in headers])

    file_path = get_export_path(f"export_{domain_id}_{datetime.now().timestamp()}.{file_type}")

    if file_type == 'xlsx':
        generate_xlsx(file_path, data)
    elif file_type == 'csv':
        generate_csv(file_path, data)

    return send_file(file_path)