from app.db.Models.flow_context import FlowContext


def get_upload_status(flow_id):
    return FlowContext(**dict(id=flow_id)).load()


def save_flow_context(upload_context: dict):
    flow_id = upload_context.pop('id') if id in upload_context else None
    fc = FlowContext(**dict(id=flow_id)).load()

    for k, v in upload_context.items():
        fc.__setattr__(k, v)

    return fc.save()
