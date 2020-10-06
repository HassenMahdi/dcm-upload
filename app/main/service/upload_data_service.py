from time import time

from app.db.Models.domain_collection import DomainCollection
from app.db.Models.field import TargetField
from app.main.dto.paginator import Paginator


def get_collection_total(domain_id, payload={}):
    collection = DomainCollection().db(domain_id=domain_id)
    query = {}
    projection = {'_id': 1}
    cursor = collection.find(query, projection)
    return cursor.count()


def get_collection_data(domain_id, payload={}):

    collection = DomainCollection().db(domain_id=domain_id)

    # sort_key = payload.get('sort_key', None) or 'upload_start_date'
    # sort_acn = payload.get('sort_acn', None) or -1

    # INDEX COL FOR SORT WORKAROUND
    start = time()
    collection.create_index([('_id', 1)])
    # print('CREATING INDEX TOOK ' + repr(time() - start) + 'seconds')

    # PAGINATION
    page = payload.get('page', None) or 1
    size = payload.get('size', None) or 15
    skip = (page - 1) * size

    # FOR FILTERS
    query = {}
    projection = {'_id':0}
    cursor = collection.find(query, projection)
    # cursor = cursor.sort([(sort_key, sort_acn)])

    # PAGINATION
    start = time()
    # total = collection.find(query, {}).count()
    total = 0
    print('COUNT DATA IN ' + repr(time() - start) + 'seconds')
    cursor = cursor.skip(skip).limit(size)

    start = time()
    data = list(cursor)
    print('FETCH DATA IN ' + repr(time() - start) + 'seconds')

    headers = [
        dict(headerName=tf.label, field=tf.name, type=tf.type) for tf in TargetField.get_all(domain_id=domain_id)
    ]

    return Paginator(data, page, size, total, headers=headers)