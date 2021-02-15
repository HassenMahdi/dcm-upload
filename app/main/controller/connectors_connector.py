from flask import request
from flask_restplus import Namespace, Resource

from app.main.service.upload_connectors_service import start_upload_from_connector

api = Namespace("connectors")


@api.route('/connector')
class ConnectorImport(Resource):
    def post(self):
        payload = request.json

        return start_upload_from_connector(**payload)