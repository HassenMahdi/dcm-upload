from flask_restplus import Namespace, fields


class NullableString(fields.String):
    __schema_type__ = ['string', 'null']
    __schema_example__ = 'nullable string'


class UploadDto:
    api = Namespace('upload')
    flow = api.model('flow', {
        'id': fields.String,
        'upload_status': fields.String,
        'upload_start_time': fields.DateTime,
        'upload_end_time': fields.DateTime,
        'upload_tags': fields.List(fields.String),
        'upload_errors': fields.Raw,
    })
    upload_context = api.model('upload context', {
        'id': fields.String,
        'tags': fields.List(fields.String),
        'domain_id': fields.String(required=True),
        'sheet_id': fields.String(required=True),
        'file_id': fields.String(required=True),
        'cleansing_job_id': fields.String(required=True),
    })
