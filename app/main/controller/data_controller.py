from flask import request
from flask_restplus import Resource, reqparse

from ..service.upload_data_service import get_collection_data
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
        return get_collection_data(domain_id, payload)