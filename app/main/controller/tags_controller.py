from flask_restplus import Resource

from ..service.tags_service import get_domain_tags
from ..util.dto import TagsDto

api = TagsDto.api


@api.route('/tags/<domain_id>')
class TagsResource(Resource):
    @api.response(200, 'Tags')
    @api.doc('Get all created Tags by domain id')
    def get(self, domain_id):
        return get_domain_tags(domain_id)