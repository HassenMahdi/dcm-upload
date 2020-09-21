from flask import request
from flask_restplus import Resource, reqparse

from ..util.dto import UploadDto
from ..service.upload_service import stage_upload, get_upload_status, save_flow_context, get_all_flow_contexts

api = UploadDto.api

_upload_flow = UploadDto.flow
_upload_global = UploadDto.flow
_upload_global_page = UploadDto.page_flow
_upload_flow_details = UploadDto.upload_flow_details
_upload_context = UploadDto.upload_context


@api.route('/flow')
class UploadResource(Resource):
    @api.response(201, 'Flows retrieved')
    @api.doc(
        'Get all created upload flows',
        params=dict(
            domain_id="Domain ID",sort_key="Sort Field", sort_acn="Sort Direction",
            page = "Page", size = "Size"
        )
    )
    @api.marshal_with(_upload_global_page)
    # @api.marshal_with(_upload_global)
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('domain_id', location='args')
        parser.add_argument('sort_key', location='args')
        parser.add_argument('sort_acn', type=int, location='args')
        parser.add_argument('page', type=int, location='args')
        parser.add_argument('size', type=int, location='args')
        args = parser.parse_args()
        return get_all_flow_contexts(**args)

    @api.response(201, 'Checks if given context has upload started. if not start it .returns flow id.')
    @api.doc('Checks if given context has upload started. if not start it .returns flow id.')
    @api.expect(_upload_context, validate=True)
    def post(self):
        upload_context = request.json
        return stage_upload(upload_context)

    @api.response(201, 'Create Flow')
    @api.doc('Create/updates workflow context')
    @api.expect(_upload_flow_details)
    @api.marshal_with(_upload_flow_details)
    def put(self):
        upload_context = request.json
        return save_flow_context(upload_context)


@api.route('/flow/<flow_id>')
class UploadStatusResource(Resource):
    @api.response(200, 'Status')
    @api.doc('Get Flow details')
    @api.marshal_with(_upload_flow_details)
    def get(self, flow_id):
        return get_upload_status(flow_id)


@api.route('/flow/<flow_id>/status/')
class UploadStatusResource(Resource):
    @api.response(200, 'Status')
    @api.doc('Get Flow Status by id')
    @api.marshal_with(_upload_flow)
    def get(self, flow_id):
        return get_upload_status(flow_id)