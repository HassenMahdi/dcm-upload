from flask import request
from flask_restplus import Namespace, Resource

from app.main.service.upload_connectors_service import start_upload_from_connector, get_job_by_id, \
    start_upload_from_connector_job

api = Namespace("connectors")


@api.route('/connector')
class ConnectorImport(Resource):
    def post(self):
        payload = request.json

        return start_upload_from_connector_job(**payload)


@api.route('/connector/<job_id>')
class JobStatus(Resource):

    def get(self, job_id):
        return get_job_by_id(job_id)
