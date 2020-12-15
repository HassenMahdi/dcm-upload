from app.db.Models.flow_context import FlowContext


def get_domain_tags(domain_id):
    cursor = FlowContext().db().aggregate([
        {"$match": {"domain_id": domain_id}},
        {"$project": {"upload_tags": 1}},
    ])

    tags_set = set()
    for i in cursor:
        for tag in i.get("upload_tags", []):
            tags_set.add(tag)

    return list(tags_set)


def get_tags_by_ids(ids):
    return {d['_id']: d.get('upload_tags', []) for d in FlowContext().db().find({"_id": {"$in": ids}}, {"upload_tags"})}


def delete_tag(domain_id, tag):
    FlowContext().db().update_many(
        {'domain_id': domain_id},
        {'$pull': {'upload_tags': tag}}
    )


def update_tag(domain_id, tag, new_value):
    FlowContext().db().update_many(
        {'domain_id': domain_id, 'upload_tags':tag},
        {'$set': {'upload_tags.$': new_value}}
    )