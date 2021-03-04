import os
import threading
import traceback

from connectors.connector import Connector
from connectors.connector_factory import ConnectorFactory
from flask import current_app, copy_current_request_context
import pandas as pd

from app.db.Models.connector import ConnectorSetup
from app.db.Models.connector_job import JobDocument


def start_upload_from_connector(job_id, **args):
    # Set job as running
    JobDocument().set_as_running(job_id)
    try:
        # LOAD DATA
        file_id = args['file_id']
        sheet_id = args['sheet_id']
        file_path = os.path.join(current_app.config["UPLOAD_FOLDER"], 'imports', file_id, f'{sheet_id}.csv')
        df = pd.read_csv(file_path, error_bad_lines=False, engine="c", dtype=str, skipinitialspace=True,
                         na_filter=False,
                         delimiter=";")

        con_info = ConnectorSetup().load(query={"_id": args["connector_id"]}).__dict__

        connector: Connector = ConnectorFactory.get_data(con_info)

        connector.upload_df(df, **args)

        # set job result
        JobDocument().set_job_data(job_id)
        # Set job as done
        JobDocument().set_as_done(job_id)

    except Exception as e:
        traceback.print_stack()
        JobDocument().set_as_error(job_id, str(e))

    # return {"status": "success", "message": "Data Uploaded"}, 200


def start_upload_from_connector_job(**args):
    job = JobDocument().save_job()
    job_id = job._id

    # Set job as started
    JobDocument().set_as_started(job_id)

    try:
        # START THREAD CONTEXTUAL
        @copy_current_request_context
        def ctx_bridge():
            start_upload_from_connector(job_id, **args)

        thr = threading.Thread(target=ctx_bridge)
        thr.start()
        # THREAD END
    except Exception as e:
        traceback.print_stack()
        JobDocument().set_as_error(job_id, str(e))

    return {"job_id": job_id}




def get_job_by_id(job_id):
    return JobDocument().get_one(job_id)
