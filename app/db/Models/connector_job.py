from app.db.connection import mongo
from app.main.util.strings import generate_id


class STATUS:
    NOT_STARTED = 'NOT_STATED'
    STARTED = 'STARTED'
    RUNNING = 'RUNNING'
    DONE = 'DONE'
    ERROR = 'ERROR'


class ConJob:
    def __init__(self):
        self._id = None
        self.job_status = None
        self.job_end_time = None
        self.job_errors = None

        self.result = None

    def to_dict(self):
        return self.__dict__

    def not_started(self):
        return self.job_status == STATUS.NOT_STARTED


class JobDocument:
    def save_job(self):
        job = ConJob()
        job._id = generate_id()
        job.job_status = STATUS.NOT_STARTED

        mongo.db.con_jobs.insert(job.to_dict())

        return job

    def get_one(self, id):
        job = mongo.db.con_jobs.find_one({'_id': id})
        return job

    def set_as_started(self, _id):
        job = self.get_one(_id)
        job['job_status'] = STATUS.STARTED
        mongo.db.con_jobs.save(job)
        return job

    def set_as_running(self, _id):
        job = self.get_one(_id)
        job['job_status'] = STATUS.RUNNING
        mongo.db.con_jobs.save(job)
        return job

    def set_as_done(self, _id):
        job = self.get_one(_id)
        job['job_status'] = STATUS.DONE
        mongo.db.con_jobs.save(job)
        return job

    def set_as_error(self, _id, errors=None):
        job = self.get_one(_id)
        job['job_status'] = STATUS.ERROR
        job['errors'] = errors
        mongo.db.con_jobs.save(job)
        return job

    def set_job_data(self, _id):
        job = self.get_one(_id)

        job["result"] = {"status": "success", "message": "Data Uploaded"}

        mongo.db.con_jobs.save(job)
        return job
