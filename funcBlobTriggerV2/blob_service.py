from azure.storage.blob import BlobServiceClient


def download_blob_from_azure(account_name, account_key, container_name, blob_name, download_path):
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key}"
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string, container_name=container_name)
    container_client = blob_service_client.get_container_client(container=container_name)
    blob_client = container_client.get_blob_client(blob_name)
    properties = blob_client.get_blob_properties()
    print(f'Propriedades: {properties}')
    with open(download_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
        print (f'Blob {blob_name} baixado com sucesso para {download_path}')

def upload_blob_to_azure(account_name, account_key, container_name, blob_name, upload_path):
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key}"
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string, container_name=container_name)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=upload_path)
    
    with open(file=blob_name, mode="rb") as data: 
        blob_client.upload_blob(data)
        print(f"\nUpload to Azure Storage as blob: {blob_name}\n\t" + upload_path)

def delete_blob_in_azure(account_name, account_key, container_name, blob_name):
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key}"
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string, container_name=container_name)
    container_client = blob_service_client.get_container_client(container=container_name)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.delete_blob()
    print (f'Blob {blob_name} deletado com sucesso para {container_name}')





