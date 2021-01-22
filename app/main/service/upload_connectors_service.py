import os

from connectors.connector import Connector
from connectors.connector_factory import ConnectorFactory
from flask import current_app
import pandas as pd

from app.db.Models.connector import ConnectorSetup


def start_upload_from_connector(**args):
    # LOAD DATA
    file_id = args['file_id']
    sheet_id = args['sheet_id']
    file_path = os.path.join(current_app.config["UPLOAD_FOLDER"], 'imports', file_id, f'{sheet_id}.csv')
    df = pd.read_csv(file_path, error_bad_lines=False,engine="c", dtype=str, skipinitialspace=True, na_filter=False,delimiter=";")

    con_info = ConnectorSetup().load(query={"_id": args["connector_id"]}).__dict__

    connector: Connector = ConnectorFactory.get_data(con_info)

    connector.upload_df(df, **args)

    return {"status": "success", "message": "Data Uploaded"}, 200
