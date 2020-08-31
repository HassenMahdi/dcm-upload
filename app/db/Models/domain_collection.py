import traceback

from app.db.connection import mongo
from app.db.document import Document


class DomainCollection(Document):
    __TABLE__ = "collection_data"

    def db(self, **kwargs):
        domain_id = kwargs['domain_id']
        return mongo.db[f"{domain_id}.{self.__TABLE__}"]

    @classmethod
    def bulk_ops(cls, ops, **kwargs):
        if len(ops) > 0:
            try:
                cls().db(**kwargs).bulk_write(ops)
            except Exception as bwe:
                traceback.print_stack()
                raise bwe

        return

    @classmethod
    def start_session(cls):
        return mongo.cx.start_session()

