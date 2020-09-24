import traceback

from app.db.Models.flow_context import FlowContext
from app.db.connection import mongo
from app.db.document import Document


class Modifications(Document):
    __TABLE__ = "modifications"

    columns = None
    worksheet_id = None
    domain_id = None
    line_id = None

    def __init__(self, **kwargs):
        super(Modifications, self).__init__(**kwargs)
        self.columns = self.columns or {}

    @classmethod
    def apply_modifications(cls, df, flow:FlowContext):
        worksheet_modifications = cls().get_all({"worksheet_id":flow.worksheet})
        wm:Modifications
        for wm in worksheet_modifications:
            line_id = int(wm.line_id)
            for column, data in wm.columns.items():
                try:
                    new_value = data.get('new', None)
                    df[column][line_id] = new_value
                except Exception as e:
                    print(f"FAILED TO APPLY MODS ON {column} LINE {line_id}")
                    print(e)
        return


