import time
import traceback
from datetime import datetime, timedelta
from io import BytesIO

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.storage.blob import BlobServiceClient, BlobClient
from flask import current_app as app


conn_str = "BlobEndpoint=https://devdcmstorage.blob.core.windows.net/;QueueEndpoint=https://devdcmstorage.queue.core.windows.net/;FileEndpoint=https://devdcmstorage.file.core.windows.net/;TableEndpoint=https://devdcmstorage.table.core.windows.net/;SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-07-16T07:57:54Z&st=2020-07-15T23:57:54Z&spr=https&sig=4cDoQPv%2Ba%2FQyBEFcr2pVojyMj4vgsm%2Fld6l9TPveQH0%3D"

def save_data_blob(data, blob_name, container_name='uploads'):
    # Instantiate a new BlobServiceClient using a connection string

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)

    # Instantiate a new ContainerClient
    container_client = blob_service_client.get_container_client(container_name)

    try:
        # Instantiate a new BlobClient
        blob_client: BlobClient = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(data)

    except:
        traceback.print_exc()


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


# path = '/path_to_blob/..'
# conn_string = <conn_string>
# blob_name = f'{path}.parquet'
#
# container = ContainerClient.from_connection_string(conn_str=conn_string, container_name=<name_of_container>)
#
# blob_client = container.get_blob_client(blob=blob_name)
# stream_downloader = blob_client.download_blob()
# stream = BytesIO()
# stream_downloader.readinto(stream)
# processed_df = pd.read_parquet(stream, engine='pyarrow')