import io
import os

from flask import current_app
import pandas as pd

from app.main.service.azure_blob_downloader_service import AzureBlobUploader
from app.main.util.custom_io import BytesIOWrapper


def start_upload_from_connector(**args):
    # LOAD DATA
    file_id = args['file_id']
    sheet_id = args['sheet_id']
    file_path = os.path.join(current_app.config["UPLOAD_FOLDER"], 'imports', file_id, f'{sheet_id}.csv')
    df = pd.read_csv(file_path, error_bad_lines=False,engine="c", dtype=str, skipinitialspace=True, na_filter=False,delimiter=";")

    # TODO OPTIMIZE
    # SWITCH TYPE get_file_stream
    output_stream = None
    output_type = args.get('file_type', 'csv')
    if output_type == 'csv':
        string_stream = io.StringIO()
        df.to_csv(path_or_buf=string_stream, sep=';')
        output_stream = BytesIOWrapper(string_stream)
    elif output_type == 'json':
        string_stream = io.StringIO()
        df.to_json(path_or_buf=string_stream, orient='records')
        output_stream = BytesIOWrapper(string_stream)
    else:
        raise Exception(f"INVALID FILE TYPE {output_type}")

    # SWITCH Connector ulaoad stream
    uploader_type = args['type']
    if uploader_type == 'UPLOAD_AZURE_STORAGE_ACCOUNT':
        conn_string = args['conn_string']
        container = args['container']
        blob_name = f"{args['blob']}.{output_type}"
        AzureBlobUploader(conn_string, container).upload_stream(blob_name, output_stream)

    return {"status": "success", "message": "Data Uploaded"}, 200
