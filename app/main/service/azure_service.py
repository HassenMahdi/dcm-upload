import traceback
from io import BytesIO

from azure.common.credentials import ServicePrincipalCredentials
from azure.core.exceptions import ResourceExistsError
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.storage.blob import BlobServiceClient, BlobClient
import pyarrow.parquet as pq
import pyarrow as pa
from flask import current_app as app

from app.main.service.azure_blob_downloader_service import AzureBlobFileDownloader


def create_blob_client():
    account_name = app.config["STORAGE_ACCOUNT_NAME"]
    account_access_key = app.config["STORAGE_ACCOUNT_KEY"]
    blobclient = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                   credential=account_access_key)
    return blobclient


def get_container(container_name=None, create=False):
    """Gets a client to interact with the specified container"""
    blob_service_client = create_blob_client()

    try:
        blob_service_client.create_container(container_name)
    except ResourceExistsError as e:
        print(f"Container {container_name} already exists!")

    container_client = blob_service_client.get_container_client(container_name)

    return container_client


def save_data_blob(data, blob_name):
    """Uploads data on the blob storage under blob name"""

    container_client = get_container(create=True)

    try:

        blob_client: BlobClient = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(data)

    except:
        traceback.print_exc()


def save_file_blob(path, container_name, blob_name):
    """Uploads data on the blob storage under blob name"""

    container_client = get_container(container_name=container_name)

    try:

        blob_client: BlobClient = container_client.get_blob_client(blob_name)
        with open(path, "rb") as data:
            blob_client.upload_blob(data, blob_type="BlockBlob")
    except:
        traceback.print_exc()


def download_data_as_table(domain_id, files_to_download=None):
    """Gets the blob's data"""

    azure_blob_file_downloader = AzureBlobFileDownloader()
    tables = azure_blob_file_downloader.download_all_blobs_in_container(prefix=f'{domain_id}/',
                                                                        filter=files_to_download)

    if len(tables) > 0:
        return pa.concat_tables(tables, promote=True)
    else:
        return pa.table([])


def parquet_to_sql(flow):
    """Copies data from parquet file to target SQL database"""
    subscription_id = app.config["AD_SUB_ID"]
    client_id = app.config["AD_CLIENT_ID"]
    secret = app.config["AD_SECRET"]
    tenant = app.config["AD_TENANT"]

    rg_name = 'Datacapture'
    df_name = 'dcm-factory'
    pip_name = "copyParquetToSql"

    parameters = {'blob_name': f"{flow.domain_id}.{flow.id}"}

    adf_client = DataFactoryManagementClient(
        ServicePrincipalCredentials(client_id, secret, tenant=tenant),
        subscription_id
    )
    run_response = adf_client.pipelines.create_run(rg_name, df_name, pip_name, parameters=parameters)
    return run_response.run_id


def get_all_blobs_container(container):
    container_client = get_container(container_name=container)
    generator = container_client.list_blobs()
    res = []
    for blob in generator:
        res.append({
            "Key": blob.name,
            "LastModified": blob.last_modified,
            "Size": blob.size,
            "StorageClass": blob.blob_type
        })
    return res