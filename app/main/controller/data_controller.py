from flask import request
from flask_restplus import Resource, reqparse

from ..service.upload_data_service import get_collection_data, get_collection_total, export_collection_data
from ..util.dto import DataDto

api = DataDto.api

_data_page = DataDto.page


@api.route('/data/<domain_id>')
class UploadResource(Resource):

    @api.response(200, 'Data retrieved.')
    @api.doc('Retrieve data from domain.')
    @api.marshal_with(_data_page)
    def post(self, domain_id):
        payload = request.json
        return get_collection_data(domain_id, payload)\


@api.route('/data/<domain_id>/total')
class UploadResourceTotal(Resource):
    @api.response(200, 'Data Count retrieved.')
    @api.doc('Retrieve data count from domain.')
    # @api.marshal_with(_data_page)
    def get(self, domain_id):
        # payload = request.json
        return get_collection_total(domain_id)


@api.route('/data/<domain_id>/export/<file_type>')
class UploadResourceExport(Resource):
    @api.response(200, 'Data Count retrieved.')
    @api.doc('Retrieve data count from domain.')
    # @api.marshal_with(_data_page)
    def post(self, domain_id, file_type):
        payload = request.json
        return export_collection_data(domain_id, payload or {}, file_type= file_type or 'xlsx')