import os
from datetime import datetime
from time import time

from flask import send_file
from app.db.Models.field import TargetField, FlowTagField
from app.db.Models.flow_context import FlowContext
from app.main.dto.paginator import Paginator
from app.main.service.azure_service import download_data_as_table
from app.main.service.tags_service import get_tags_by_ids
from app.main.util.file_generators import generate_xlsx, generate_csv
from app.main.util.parquet import iter_parquet
from app.main.util.storage import get_export_path

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def get_paginated_data(domain_id, filters=None,limit=None, skip=None):

    tags = next((f["value"] for f in filters if f["column"] == "flow_tags"), None)
    files_to_download = FlowContext.get_flow_files(domain_id, tags)

    """Gets the page data"""
    table = download_data_as_table(domain_id, files_to_download)

    # Calculate Row Indicies
    page_indices, all_indices = get_data_indices(table, filters, None, skip, limit)

    if page_indices is not None:
        if len(page_indices) > 0:
            table = table.take(list(page_indices))
        else:
            table = pa.table([])

    # ADD UPLOAD TAGES HERE FROM MONGO
    table = append_tags(table)

    return iter_parquet(table), len(all_indices)


def append_tags(table):
    if "flow_id" in table.column_names:
        tags = get_tags_by_ids(table.column('flow_id').unique().to_pylist())
        tags_array_like = [tags.get(f_id, []) for f_id in table.column('flow_id').to_pylist()]
        table = table.append_column(FlowTagField.name, pa.array(tags_array_like))
    return table



def get_collection_data(domain_id, payload={}):

    page = payload.get('page', None) or 1
    limit = payload.get('size', None) or 15
    filters = payload.get('filters', None)
    skip = (page - 1) * limit
    fields = TargetField.get_all(domain_id=domain_id)

    cursor, total = get_paginated_data(domain_id,filters, limit, skip)
    fields.append(FlowTagField)
    headers = [dict(headerName=tf.label, field=tf.name, type=tf.type) for tf in fields]
    data = []
    for row in cursor:
        data.append({f.name: f.format_value(row.get(f.name, None)) for f in fields})

    return Paginator(data, page, limit, total, headers=headers)


def export_collection_data(domain_id, payload={}, file_type='xlsx'):
    headers = TargetField.get_all(domain_id=domain_id)
    cursor, total = get_paginated_data(domain_id,payload.get('filters', None), payload.get("page", None), payload.get("size", None))

    data = [[h.label for h in headers]]
    for row in cursor:
        data.append([h.format_value(row.get(h.name, None)) for h in headers])

    file_path = get_export_path(f"export_{domain_id}_{datetime.now().timestamp()}.{file_type}")

    if file_type == 'xlsx':
        generate_xlsx(file_path, data)
    elif file_type == 'csv':
        generate_csv(file_path, data)

    return send_file(file_path)


def get_data_indices(table, filters, sort, skip, limit):
    """Applies the filters on mapped_df for data preview"""
    row_indices = range(0, len(table))

    if filters and len(filters) > 0:
        filter_columns = [col_filter["column"] for col_filter in filters if ( col_filter['column'] in table.column_names) ]
        df = table.select(filter_columns).to_pandas()
        date_operators = {
            'date.lessThan': lambda s,v : s < v,
            'date.greaterThan': lambda s,v : s > v,
            'date.equals':lambda s,v : s == v,
            'date.notEquals':lambda s,v : s != v,
            'date.inRange':lambda s,v : (s <= pd.to_datetime(value['max'])) & (s >= pd.to_datetime(value['min']))
        }
        numeric_operators = {
            'lessThan':  lambda s,v : s < v,
            'lessThanOrEqual':  lambda s,v : s <= v,
            'greaterThanOrEqual': lambda s,v : s >= v,
            'greaterThan': lambda s,v : s > v,
            'inRange': lambda s,v : (s <= value['max']) & (s >= value['min'])
        }

        for column_filter in filters:
            column = column_filter["column"]
            operator = column_filter["operator"]
            value = column_filter.get("value")
            if operator in date_operators.keys():
                func = date_operators.get(operator)
                date_values = pd.to_datetime(df[column], errors='coerce')
                mask = func(date_values, value)
                df = df[mask]
            elif operator in numeric_operators.keys():
                func = numeric_operators.get(operator)
                numeric_values = pd.to_numeric(df[column], errors='coerce')
                value = value
                mask = func(numeric_values, value)
                df = df[mask]
            elif operator == 'equals':
                df = df.loc[df[column] == value]
            elif operator == 'notEquals':
                df = df.loc[df[column] != value]
            elif operator == 'contains':
                df = df.loc[df[column].str.contains(value)]
            elif operator == 'notContains':
                df = df.loc[~df[column].str.contains(value)]
            elif operator == 'startsWith':
                df = df.loc[df[column].str.startswith(value)]
            elif operator == 'endsWith':
                df = df.loc[df[column].str.endswith(value)]
            elif operator == date_operators:
                df = df.loc[getattr(pd.to_datetime(df[column], errors='coerce'), date_operators[operator])
                            (pd.to_datetime(value))]

        row_indices = df.index.tolist()
    # TODO
    # Do Sort Here

    page_indices = row_indices
    if skip is not None and limit is not None:
        start = skip
        end = min(start + limit, len(table))
        page_indices = row_indices[start: end]

    return page_indices, row_indices
