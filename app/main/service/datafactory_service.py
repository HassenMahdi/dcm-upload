import traceback

from azure.storage.blob import BlobServiceClient, BlobClient

conn_str = "BlobEndpoint=https://devdcmstorage.blob.core.windows.net/;QueueEndpoint=https://devdcmstorage.queue.core.windows.net/;FileEndpoint=https://devdcmstorage.file.core.windows.net/;TableEndpoint=https://devdcmstorage.table.core.windows.net/;SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-07-16T07:57:54Z&st=2020-07-15T23:57:54Z&spr=https&sig=4cDoQPv%2Ba%2FQyBEFcr2pVojyMj4vgsm%2Fld6l9TPveQH0%3D"


def copy_parquet_blob(original_path, blob_name, container_name='uploads'):
    # Instantiate a new BlobServiceClient using a connection string

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)

    # Instantiate a new ContainerClient
    container_client = blob_service_client.get_container_client(container_name)

    try:
        # Instantiate a new BlobClient
        blob_client: BlobClient = container_client.get_blob_client(blob_name)
        # blob_client.create_append_blob()
        with open(original_path, "rb") as data:
            blob_client.upload_blob(data)

    except:
        traceback.print_exc()
