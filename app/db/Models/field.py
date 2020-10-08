from app.db.Models.flow_context import FlowContext
from app.db.connection import mongo
from app.db.document import Document

import pandas as pd

class TargetField(Document):

    __TABLE__ = "fields"

    def db(self, **kwargs):
        domain_id = kwargs['domain_id']
        return mongo.db[f"{domain_id}.{self.__TABLE__}"]

    name = None
    description = None
    label = None
    created_on = None
    modified_on = None
    type = None
    category = None
    rules = None
    editable = None
    mandatory = None

    @classmethod
    def apply_types(cls, frame, flow: FlowContext):
        fields = cls.get_all({}, domain_id=flow.domain_id)
        f: TargetField
        for f in fields:
            if f.name in frame.columns:
                if f.type in ['date']:
                    frame[f.name] = pd.to_datetime(frame[f.name])
                if f.type in ['double', 'int']:
                    frame[f.name] = pd.to_numeric(frame[f.name])


