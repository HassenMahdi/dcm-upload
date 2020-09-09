import traceback

from app.db.connection import mongo
from app.db.document import Document


class Modifications(Document):
    __TABLE__ = "modifications"

    columns = None
    worksheet_id = None
    domain_id = None

    def __init__(self, **kwargs):
        super(Modifications, self).__init__(**kwargs)
        self.columns = self.columns or {}

    def load(self, query=None, **kwargs):
        query = query or {'worksheet_id': self.worksheet_id}
        super(Modifications, self).load(query, **kwargs)
        return self

    def apply_modifications(self, frame):
        if len(self.columns) > 0:
            for key in self.columns.keys():
                for row_index, value in self.columns[key].items():
                    if key in frame.columns and int(row_index) in frame.index:
                        frame[key][int(row_index)] = value
        return

