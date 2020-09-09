import os
from datetime import datetime

from app.db.document import Document

from flask import current_app


class STATUS:
    NOT_STATED = 'NOT_STATED'
    STARTED = 'STARTED'
    RUNNING = 'RUNNING'
    DONE = 'DONE'
    ERROR = 'ERROR'


class FlowContext(Document):
    __TABLE__ = "flow"

    @property
    def filepath(self):
        if self.transformation_id:
            # TODO GENERATE WITH PIPE DATA
            return self.cleansing_job_id.replace('_job','.csv').replace('imports','mappings')
        else:
            return os.path.join(current_app.config["UPLOAD_FOLDER"], 'mappings', self.file_id, f'{self.sheet_id}.csv')

    @property
    def worksheet(self):
        if self.transformation_id:
            # TODO GENERATE WITH PIPE DATA
            return self.transformation_id.split('/')[-1]
        else:
            return self.sheet_id

    # IDENTIFIERS
    domain_id = None
    transformation_id = None
    sheet_id = None
    file_id = None
    cleansing_job_id = None

    latest_step = None
    upload_status = STATUS.NOT_STATED
    upload_start_time = None
    upload_end_time = None
    upload_tags = None
    upload_errors = None
    total_records = 0
    inserted_records = 0
    columns = None
    previous_status = None

    store = None

    def set_status(self, status):
        self.previous_status = self.previous_status or []
        self.previous_status.append(self.upload_status)

        self.upload_status = status
        return self

    def not_started(self):
        return self.upload_status == STATUS.NOT_STATED

    def set_as_started(self, **kwargs):
        self.latest_step = 'UPLOAD'
        self.upload_tags = kwargs.get('tags', [])
        self.upload_start_time = datetime.now()
        self.set_status(STATUS.STARTED)
        return self

    def set_as_running(self):
        self.set_status(STATUS.RUNNING)
        return self

    def set_as_error(self, errors = None):
        self.upload_errors = errors
        self.upload_end_time = datetime.now()
        self.set_status(STATUS.ERROR)
        return self

    def set_as_done(self):
        self.upload_end_time = datetime.now()
        self.set_status(STATUS.DONE)
        return self

    def set_upload_meta(self, total_record, columns):
        self.total_records = total_record
        self.columns = columns
        return self

    def append_inserted_and_save(self, inserted):
        self.db().update_one({"_id":self.id}, {"$inc":{"inserted_records": inserted}})
        self.inserted_records += inserted
        return self

