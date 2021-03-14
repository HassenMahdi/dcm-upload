import pandas as pd

from app.main.service.aws_service import put_in_s3
from app.main.service.azure_service import save_file_blob, get_all_blobs_container
from app.main.service.excel import generate_csv_from_excel
from app.main.service.upload_mysql_service import sink_to_mysql
from app.main.service.user_service import get_a_user
import uuid
from flask import current_app as app
import json
from pathlib import Path
import os


def main_automatic(context):
    identifier = str(uuid.uuid4().fields[-1])[:5]
    filepath = app.config['UPLOAD_FOLDER'] + "/result_files/" + context["uid"] + '/' + context["filename"]
    filename = Path(filepath).stem
    out_filename = context["out_filename"].replace(" ", "_")
    user_container = get_a_user(context["uid"]).username

    if "template" in context and context["template"] is not None and context["template"] is True:
        filepath = generate_csv_from_template(filepath, filename, context["template_id"], identifier)

    df_original = pd.read_csv(filepath, sep=";")
    df = df_original.fillna('')
    for out in context["outputs"]:
        if out == "sql":
            inject_in_sql(df, context["uid"], filename, identifier, out_filename)
        elif out == "json":
            generate_json(df, filename, identifier, out_filename, user_container)
        elif out == "csv":
            generate_csv(df, filename, identifier, out_filename, user_container)
        elif out == "parquet":
            generate_parquet(df_original, filename, identifier, out_filename, user_container)
        else:
            raise Exception("Output Type Not Recognized!")


def inject_in_sql(df, user_id, tablename, identifier, out_tbname):
    try:
        bdd = get_a_user(user_id).userDb
        sink_to_mysql(df, out_tbname + '-' + identifier, bdd)
    except Exception as e:
        raise e


def generate_json(df, filename, id, out_fname, container):
    try:
        output = df.to_dict("list")
        unique_filename = filename + '-' + id + ".json"
        path = app.config['UPLOAD_FOLDER'] + "/temp/" + unique_filename
        with open(path, 'w', encoding='utf-8') as fp:
            json.dump(output, fp)
        # put_in_s3(path, out_fname + ".json")
        save_file_blob(path, container, out_fname + ".json")
    except Exception as e:
        raise e


def generate_csv(df, filename, id, out_fname, container):
    try:
        unique_filename = filename + '-' + id + ".csv"
        path = app.config['UPLOAD_FOLDER'] + "/temp/" + unique_filename
        df.to_csv(path_or_buf=path, index=False)
        # put_in_s3(path, out_fname + ".csv")
        save_file_blob(path, container, out_fname + ".csv")
    except Exception as e:
        raise e


def generate_parquet(df, filename, id, out_fname, container):
    try:
        unique_filename = filename + '-' + id + ".parquet"
        path = app.config['UPLOAD_FOLDER'] + "/temp/" + unique_filename
        df.to_parquet(path=path, index=False)
        # put_in_s3(path, out_fname + ".parquet")
        save_file_blob(path, container, out_fname + ".parquet")
    except Exception as e:
        raise e


def upload_file(request, uid):
    """Retrieves the file from request.files property and saves it under UPLOAD FOLDER defined in config file"""

    try:
        if 'file' not in request.files:
            return {"uploaded": False, "message": 'No file part'}

        file = request.files['file']
        filename = file.filename
        if filename == '':
            return {"uploaded": False, "message": "No selected file"}

        user_directory = app.config['UPLOAD_FOLDER'] + "/result_files/" + uid
        if not os.path.exists(user_directory):
            os.makedirs(user_directory)

        file.save(user_directory + '/' + filename)

        return {"uploaded": True, "message": "OK"}
    except Exception as e:
        print(e)
        return {"uploaded": False, "message": "Error"}


def generate_csv_from_template(filepath, filename, template_id, identifier):
    try:
        unique_filename = filename + ".csv"
        outpath = app.config['UPLOAD_FOLDER'] + "/temp/" + unique_filename
        return generate_csv_from_excel(filepath, outpath, template_id)
    except Exception as e:
        raise e


def get_user_uploaded_files(uid):
    user_container = get_a_user(uid).username
    return get_all_blobs_container(user_container)
