import traceback
from io import BytesIO

from azure.common.credentials import ServicePrincipalCredentials
from azure.core.exceptions import ResourceExistsError
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.storage.blob import BlobServiceClient, BlobClient
from flask import current_app as app
import pyarrow.parquet as pq
import pyarrow as pa


def get_container(container_name="uploads", create = False):
    """Gets a client to interact with the specified container"""

    conn_str = app.config["ASA_URI"]
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)

    if create:
        try:
            # Attempt to create container
            blob_service_client.create_container(container_name)
        # Catch exception and print error
        except ResourceExistsError as error:
            # Container foo does not exist. You can now create it.
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


def download_data_as_table(domain_id):
    """Gets the blob's data"""

    container_client = get_container()

    # byte_stream = io.BytesIO()
    # block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
    # try:
    #     block_blob_service.get_blob_to_stream(container_name=container_name, blob_name=parquet_file,
    #                                           stream=byte_stream)
    #     df = pq.read_table(source=byte_stream).to_pandas()
    #     # Do work on df ...
    # finally:
    #     # Add finally block to ensure closure of the stream
    #     byte_stream.close()

    blob_list = container_client.list_blobs(name_starts_with=f'{domain_id}/')
    tables = []
    for blob in blob_list:
        blob_client = container_client.get_blob_client(blob=blob.name)
        byte_stream = BytesIO()
        blob_client.download_blob().readinto(byte_stream)
        tables.append(pq.read_table(source=byte_stream))
        byte_stream.close()

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