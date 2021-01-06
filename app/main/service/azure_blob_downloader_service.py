# download_blobs_parallel.py
# Python program to bulk download blobs from azure storage
# Uses latest python SDK() for Azure blob storage
# Requires python 3.6 or above

import os
from io import BytesIO
from multiprocessing.pool import ThreadPool
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient
import pyarrow.parquet as pq

from flask import current_app as app


class AzureBlobFileDownloader:
    def __init__(self):
        self.MY_CONNECTION_STRING = app.config["ASA_URI"]
        self.MY_BLOB_CONTAINER = "uploads"

        # Initialize the connection to Azure storage account
        self.blob_service_client = BlobServiceClient.from_connection_string(self.MY_CONNECTION_STRING)
        self.my_container = self.blob_service_client.get_container_client(self.MY_BLOB_CONTAINER)

    def download_all_blobs_in_container(self, prefix=None, filter=None):
        # get a list of blobs
        my_blobs = self.my_container.list_blobs(name_starts_with=prefix)

        if filter is not None:
            def valid_blob(blob,filter):
                for f in filter:
                    if f in blob.name:
                        return True
                return False
            my_blobs = (blob for blob in my_blobs if valid_blob(blob,filter))
        result = self.run(my_blobs)
        return result

    def run(self, blobs):
        # Download 10 files at a time!
        with ThreadPool(processes=int(10)) as pool:
            return pool.map(self.get_parquet_table, blobs)

    def get_parquet_table(self, blob):
        blob_client = self.my_container.get_blob_client(blob=blob.name)
        byte_stream = BytesIO()
        blob_client.download_blob().readinto(byte_stream)
        partition = pq.read_table(source=byte_stream)
        byte_stream.close()
        return partition


class AzureBlobUploader():
    def __init__(self, conn_string, container):
        self.MY_CONNECTION_STRING = conn_string
        self.MY_BLOB_CONTAINER = container

        # Initialize the connection to Azure storage account
        self.blob_service_client = BlobServiceClient.from_connection_string(self.MY_CONNECTION_STRING)
        self.my_container = self.blob_service_client.get_container_client(self.MY_BLOB_CONTAINER)

    def upload_stream(self, blob_name, stream):
        blob_client: BlobClient = self.my_container.get_blob_client(blob_name)

        # Clear Old Blob
        try:
            blob_client.delete_blob()
        except:
            print("Blob does not exist")

        blob_client.upload_blob(stream)
