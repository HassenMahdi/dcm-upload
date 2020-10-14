mongo_queries = dict(
    contains=lambda c, v: {c: {'$regex': f"{v}"}},
    notContains=lambda c, v: {c: {'$not': {'$regex': f"{v}"}}},
    equals=lambda c, v: {c: {'$eq': v}},
    greaterThan=lambda c, v: {c: {'$gt': v}},
    greaterThanOrEqual=lambda c, v: {c: {'$gte': v}},
    lessThan=lambda c, v: {c: {'$lt': v}},
    lessThanOrEqual=lambda c, v: {c: {'$lte': v}},
    notEquals=lambda c, v: {c: {'$not': {'$eq': v}}},
    startsWith=lambda c, v: {c: {'$regex': f"^{v}"}},
    endsWith=lambda c, v: {c: {'$regex': f"{v}$"}},
    inRange=lambda c, v: {c: {'$range': f"{v}$"}},
    set=lambda c, v: {c: {'$in': v}},
)


def filters_to_query(filters):
    query={}
    for f in filters:
        try:
            query.update(mongo_queries.get(f['operator'])(f['column'], f['value']))
        except Exception as e:
            print("Enable to find converter")
            print(e)

    print(query)
    return query
