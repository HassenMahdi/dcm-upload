from flask_restplus import Namespace, fields


class NullableString(fields.String):
    __schema_type__ = ['string', 'null']
    __schema_example__ = 'nullable string'


class UploadDto:
    api = Namespace('upload')
    flow = api.model('flow', {
        'id': NullableString,
        'upload_status': fields.String,
        'upload_start_time': fields.DateTime,
        'upload_end_time': fields.DateTime,
        'upload_tags': fields.List(fields.String),
        'upload_errors': fields.Raw,
        'total_records': fields.Integer,
        'inserted_records': fields.Integer,
        'columns': fields.List(fields.Raw),
        'previous_status': fields.List(fields.Raw),
        'transformation_id': fields.String,
        'sheet_id' : fields.String,
        'user': fields.Raw
    })
    page_flow = api.model('flow page', {
        'page':fields.Integer,
        'size':fields.Integer,
        'total':fields.Integer,
        'content':fields.List(fields.Nested(flow)),
    })
    upload_context = api.model('upload context', {
        'id': NullableString,
        'tags': fields.List(fields.String),
        'domain_id': fields.String(required=True),
        'sheet_id': fields.String(required=True),
        'file_id': fields.String(required=True),
        'cleansing_job_id': fields.String(required=True),
        'transformation_id': NullableString,
        'pipe_id': NullableString(description='Transformation Pipe Id'),
        'mapping_id': fields.String(description='Mapping Id'),
        'user_id': fields.String(description='User Id')
    })
    upload_flow_details = api.model('upload flow details', {
        'id': NullableString,
        'upload_tags': fields.List(fields.String),
        'domain_id': fields.String,
        'sheet_id': fields.String,
        'file_id': fields.String,
        'cleansing_job_id': fields.String,
        'transformation_id': NullableString,
        'pipe_id': NullableString(description='Transformation Pipe Id'),
        'mapping_id': NullableString(description='Mapping Id'),
    })


class DataDto:
    api = Namespace('data')
    page = api.model('Data Page', {
        'page': fields.Integer,
        'size': fields.Integer,
        'total': fields.Integer,
        'content': fields.List(fields.Raw),
        'headers': fields.List(fields.Raw),
    })


class TagsDto:
    api = Namespace('tags')
