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
            return f"{self.transformation_id}.csv"
        return os.path.join(current_app.config["UPLOAD_FOLDER"], 'imports', self.file_id, f'{self.sheet_id}.csv')

    # IDENTIFIERS
    domain_id = None
    transformation_id = None
    sheet_id = None
    file_id = None
    cleansing_job_id = None

    tags = None

    latest_step = None
    upload_status = STATUS.NOT_STATED
    upload_start_time = None
    upload_end_time = None
    upload_tags = None
    upload_errors = None
    total_record = 0
    inserted_records = 0

    store = None

    def not_started(self):
        return self.upload_status == STATUS.NOT_STATED

    def set_as_started(self, **kwargs):
        self.latest_step = 'UPLOAD'
        self.upload_tags = kwargs.get('tags', [])
        self.upload_start_time = datetime.now()
        self.upload_status = STATUS.STARTED
        return self

    def set_as_running(self):
        self.upload_status = STATUS.RUNNING
        return self

    def set_as_error(self, errors = None):
        self.upload_errors = errors
        self.upload_end_time = datetime.now()
        self.upload_status = STATUS.ERROR
        return self

    def set_as_done(self):
        self.upload_end_time = datetime.now()
        self.upload_status = STATUS.DONE
        return self

    def set_upload_meta(self, total_record):
        self.total_record = total_record
        return self

    def append_inserted_and_save(self, inserted):
        self.db().update_one({"_id":self.id}, {"$inc":{"inserted_records": inserted}})
        return self

