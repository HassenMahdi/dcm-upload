from app.db.Models.flow_context import FlowContext
from app.db.connection import mongo
from app.db.document import Document

from datetime import datetime

import pandas as pd
import numpy as np

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

    def format_value(self, scalar_value):
        value = str(scalar_value)
        if value == "None":
            return ''

        if self.type == 'date':
            try:
                d=datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            except:
                d = value
            try:
                return d.strftime("%Y-%m-%d")
            except Exception as e:
                return value

        elif self.type == FlowTagField.type:
            return [str(tag) for tag in scalar_value or []]
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
                    frame[column] = pd.to_numeric(frame[column], errors='coerce', downcast='float')\
                        .fillna(0).astype(np.float)
                if f.type in ['int']:
                    frame[column] = pd.to_numeric(frame[column], errors='coerce', downcast='integer')\
                        .fillna(0).astype(np.int64)


FlowTagField = TargetField(**dict(
    label='Tags',
    name='flow_tags',
    type='flow_tags',
))