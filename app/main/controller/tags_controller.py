from flask import request
from flask_restplus import Resource

from app.main.service.automatic_upload import main_automatic, upload_file
from app.main.service.aws import main_s3, get_all_files_in_s3
from app.main.service.excel import get_template_by_id, create_template, get_templates, update_template, \
    delete_template_by_id
from app.main.service.user_service import get_a_user
from ..service.tags_service import get_domain_tags
from ..service.upload_sql_service import sink_to_mysql
from ..util.dto import TagsDto

api = TagsDto.api


@api.route('/tags/<domain_id>')
class TagsResource(Resource):
    @api.response(200, 'Tags')
    @api.doc('Get all created Tags by domain id')
    def get(self, domain_id):
        return get_domain_tags(domain_id)


@api.route('/tags')
class TestResource(Resource):
    @api.response(200, 'Tags')
    @api.doc('Get all created Tags by domain id')
    def get(self):
        return get_a_user("1d70ce7c-f4aa-43d6-b3cf-90dc46138199")


@api.route('/automtic')
class TransformResource(Resource):
    @api.response(200, 'Tags')
    @api.doc('Get all created Tags by domain id')
    def post(self):
        context = request.json
        main_automatic(context)
        return {"status": "OK"}


@api.route('/import/<uid>')
class ImprtResource(Resource):
    @api.response(200, 'Tags')
    @api.doc('Get all created Tags by domain id')
    def post(self, uid):
        return upload_file(request, uid)


@api.route('/datalake')
class DataLakeResource(Resource):
    @api.response(200, 'Tags')
    @api.doc('Get all created Tags by domain id')
    def get(self):
        return get_all_files_in_s3()


@api.route('/template')
class TemplateResource(Resource):
    @api.response(200, 'Tags')
    @api.doc('Get all created Tags by domain id')
    def get(self):
        return get_templates()

    @api.response(200, 'Template')
    @api.doc('Get all created Tags by domain id')
    def post(self):
        data = request.json
        return create_template(data)


@api.route('/template/<temp_id>')
class TemplateUpdateResource(Resource):
    @api.response(200, 'Tags')
    @api.doc('Get all created Tags by domain id')
    def get(self,temp_id):
        return get_template_by_id(temp_id)

    @api.response(200, 'Template')
    @api.doc('Get all created Tags by domain id')
    def post(self,temp_id):
        data = request.json
        return update_template(temp_id,data)

    @api.response(200, 'Template')
    @api.doc('Get all created Tags by domain id')
    def delete(self, temp_id):
        return delete_template_by_id(temp_id)
