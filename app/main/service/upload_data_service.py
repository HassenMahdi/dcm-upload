import os
from datetime import datetime
from time import time

from flask import send_file, current_app
from pyexcelerate import Workbook, Color
from app.db.Models.domain_collection import DomainCollection
from app.db.Models.field import TargetField
from app.main.dto.paginator import Paginator
from app.main.util.file_generators import generate_xlsx, generate_csv


def get_collection_total(domain_id, payload={}):
    collection = DomainCollection().db(domain_id=domain_id)
    query = {}
    projection = {'_id': 1}
    cursor = collection.find(query, projection)
    return cursor.count()


def get_collection_cusror(domain_id, payload={}):
    collection = DomainCollection().db(domain_id=domain_id)
    collection.create_index([('_id', 1)])
    # FOR FILTERS
    query = {}
    projection = {'_id': 0}
    cursor = collection.find(query, projection)

    return cursor


def get_collection_data(domain_id, payload={}, pagination=True):

    total = 0
    cursor = get_collection_cusror(domain_id, payload)

    # PAGINATION
    if pagination:
        page = payload.get('page', None) or 1
        size = payload.get('size', None) or 15
        skip = (page - 1) * size
        cursor = cursor.skip(skip).limit(size)

    data = list(cursor)

    headers = [
        dict(headerName=tf.label, field=tf.name, type=tf.type) for tf in TargetField.get_all(domain_id=domain_id)
    ]

    return Paginator(data, page, size, total, headers=headers)


def export_collection_data(domain_id, payload={}, file_type='xlsx'):
    cursor = get_collection_cusror(domain_id, payload)
    headers = TargetField.get_all(domain_id=domain_id)

    data = [[h.label for h in headers]]
    for row in cursor:
        data.append([row.get(h.name, None) for h in headers])

    file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"exports/export_{domain_id}_{datetime.now().timestamp()}.{file_type}")

    if file_type == 'xlsx':
        generate_xlsx(file_path, data)
    elif file_type == 'csv':
        generate_csv(file_path, data)

    return send_file(file_path)