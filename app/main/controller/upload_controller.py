from flask import request
from flask_restplus import Resource

from ..util.dto import UploadDto
from ..service.upload_service import pend_upload, get_upload_status, save_flow_context, get_all_flow_contexts

api = UploadDto.api

_upload_flow = UploadDto.flow
_upload_global = UploadDto.flow
_upload_context = UploadDto.upload_context


@api.route('/flow')
class UploadResource(Resource):
    @api.response(201, 'Flows retrieved')
    @api.doc('Get all created upload flows')
    @api.marshal_list_with(_upload_global)
    def get(self):
        return get_all_flow_contexts()

    @api.response(201, 'Checks if given context has upload started. if not start it .returns flow id.')
    @api.doc('Checks if given context has upload started. if not start it .returns flow id.')
    @api.expect(_upload_context, validate=True)
    def post(self):
        upload_context = request.json
        return pend_upload(upload_context)

    @api.response(201, 'Create Flow')
    @api.doc('Create/updates workflow context')
    @api.marshal_with(_upload_global)
    def put(self):
        upload_context = request.json
        return save_flow_context(upload_context)


@api.route('/flow/<flow_id>/status/')
class UploadStatusResource(Resource):
    @api.response(200, 'Status')
    @api.doc('Get Flow Status by id')
    @api.marshal_with(_upload_flow)
    def get(self, flow_id):
        return get_upload_status(flow_id)