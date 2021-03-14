import json
import pandas as pd
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from flask import logging, jsonify
import uuid
from flask import current_app as app

from app.main.service.azure_service import save_file_blob


def put_in_s3(path, filename):
    try:
        client = get_aws_client()
        client.upload_file(path, "deepkube-data", 'deepkube-input/{}'.format(filename))
    except ClientError as e:
        logging.error(e)


def main_s3(filepathcsv, df=None,filename=None,container=None):
    # del df['flow_id']
    # data = df.fillna('')
    output = df.to_dict("list")
    id = str(uuid.uuid4())
    unique_filename = id + ".json"
    path = app.config['UPLOAD_FOLDER'] + "/temp/" + unique_filename
    with open(path, 'w', encoding='utf-8') as fp:
        json.dump(output, fp, default=myconverter)
    save_file_blob(path, container, unique_filename if not filename else f"{filename}.json")
    # put_in_s3(path, unique_filename if not filename else f"{filename}.json")
    # put_in_s3(filepathcsv, id + ".csv")


def myconverter(o):
    return o.__str__()


def get_aws_client():
    my_config = Config(
        region_name=app.config['AWS_REGION'],
        signature_version='s3v4',
        retries={
            'max_attempts': 10,
            'mode': 'standard'
        }
    )
    client = boto3.client(
        's3',
        config=my_config,
        aws_access_key_id=app.config['AWS_KEY'],
        aws_secret_access_key=app.config['AWS_SECRET']
    )
    return client


def get_all_files_in_s3():
    client = get_aws_client()
    files = client.list_objects(Bucket="deepkube-data", Prefix="deepkube-input/")
    for el in files["Contents"]:
        el.pop('ETag', None)
        el.pop('Owner', None)
    return jsonify(files["Contents"])
