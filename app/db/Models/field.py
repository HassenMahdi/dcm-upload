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

    def format_value(self, value):
        if self.type == 'date':
            try:
                return f'{value.year}-{value.month}-{value.day}'
            except Exception as e:
                return str(value)

        elif self.type == FlowTagField.type:
            return value or []
        else:
            return str(value)

    @classmethod
    def apply_types(cls, frame, flow: FlowContext):
        fields = cls.get_all({}, domain_id=flow.domain_id)
        f: TargetField
        for f in fields:
            column = f.name
            if column in frame.columns:
                if f.type in ['date']:
                    frame[column] = pd.to_datetime(frame[column], errors='coerce')
                if f.type in ['double']:
                    frame[column] = pd.to_numeric(frame[column], errors='coerce', downcast='float')
                if f.type in ['int']:
                    frame[column] = pd.to_numeric(frame[column], errors='coerce', downcast='integer')


FlowTagField = TargetField(**dict(
    label='Tags',
    name='flow_tags',
    type='flow_tags',
))