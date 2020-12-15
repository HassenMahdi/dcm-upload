from flask import request
from flask_restplus import Resource

from ..service.tags_service import get_domain_tags, update_tag, delete_tag
from ..util.dto import TagsDto

api = TagsDto.api

@api.route('/tags/<domain_id>')
class TagsResource(Resource):
    @api.response(200, 'Tags')
    @api.doc('Get all created Tags by domain id')
    def get(self, domain_id):
        return get_domain_tags(domain_id)

    @api.response(200, 'Tags')
    @api.doc('Update Tags by domain id')
    def post(self, domain_id):
        payload = request.json
        edit = payload.get('edit', False)
        tag = payload['tag']
        new_tag = payload.get('newTag', tag)
        if edit:
            return update_tag(domain_id, tag, new_tag)
        else:
            return delete_tag(domain_id, tag)


