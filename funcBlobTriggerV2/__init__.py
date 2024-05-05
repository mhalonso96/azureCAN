from azure.functions import HttpResponse
import logging
from .process_tp_data import processingTP, readConfig
from azure.functions import InputStream
import os
from .blob_service import download_blob_from_azure, upload_blob_to_azure, delete_blob_in_azure
from datetime import datetime
import tempfile
import glob
import pytz

account_name="etncanedgestorage"
account_key="N1UbsN0AuXDhaAPDrUqq+sSUxd03w7Bju/KJVQVwsffhYYJNayXbJE3NejeqtEzhwCVcsCeS4Lfy+ASt0dYQjg==;EndpointSuffix=core.windows.net"

def main(myblob: InputStream):
    utc_brazil = pytz.timezone('America/Sao_Paulo')
    path_container_upload = datetime.now(utc_brazil).strftime("%Y/%m/%d")
    blob_config = myblob.name
    # print(blob_config)
    tmpFilePath = tempfile.gettempdir()
    app_path = os.getcwd()
    dbcs_path = app_path + "/app/"


    blob_config = blob_config.split('/')
    blob_name_mf4 = blob_config[1] + "/" + blob_config[2] + "/" + blob_config[3]
    blob_name_config =  "App/config.json"
    name = blob_config[3].split('.')
    container_name = blob_config[0]
    tmp_path =  tmpFilePath + "/"
    download_path_mf4 = tmp_path + blob_config[3]
    download_path_config = tmp_path + "config.json"

    try:
        download_blob_from_azure(account_name, account_key, container_name, blob_name_config, download_path_config)
        logging.info(f'O arquivo {blob_name_config} foi baixado na pasta {download_path_config}')
    except Exception as err:
        logging.error(f'Erro encontrado: {err}')
        return
    
    try:
        download_blob_from_azure(account_name, account_key, container_name, blob_name_mf4, download_path_mf4)
        logging.info(f'O arquivo {blob_name_mf4} foi baixado na pasta {download_path_mf4}')
    except Exception as err:
        logging.error(f'Erro encontrado: {err}')
        return
    
    files = glob.glob(tmpFilePath + '/*')
    logging.info(f'Os arquivos atuais da pasta {tmpFilePath}: {files}')
    
    try:
        URL_config = f"{tmp_path}config.json"
        config = readConfig(URL_config)
        vehicle = config[4][1]['vehicle']
        #print("Diretorio .config.json", URL_config)
        dbcCANpath = os.path.join(dbcs_path, config[5][0]['J1939']).replace("\\","/")
        #print("Diretorio dbc CAN", dbcCANpath)
        dbcGPSpath = os.path.join(dbcs_path, config[5][1]['GPS']).replace("\\","/")
        #print("Diretorio dbc GPS",dbcGPSpath)
        dbc_paths = [dbcCANpath, dbcGPSpath]
        #print(f"Inicializando a conversao ..... aguarde um momento")
        csv_path = processingTP(blob_config[3], tmp_path, dbc_paths, "j1939", config[0], config[1], config[2], config[3], config[4], name[0])
        logging.info(f'O arquivo {csv_path} foi gerado com sucesso')
        #print(f'Diretorio csv: {csv_path}')
        csv_path_split = csv_path.split('//')
        
    except Exception as err:
        logging.error(f"Erro no arquivo: {err}")
        return 

    try:    
        upload_filename = f'EATON/{vehicle}/{blob_config[1]}/{path_container_upload}/{csv_path_split[1]}'
        upload_blob_to_azure(account_name, account_key, container_name, csv_path, upload_filename)
        logging.info(f'O arquivo {csv_path} foi gravado na {container_name} no seguinte diretorio {upload_filename}')
    except Exception as err:
        logging.error(f'Ocorreu um erro no upload do {csv_path}: {err}')
        return
        
    try:
        os.remove(csv_path)
        os.remove(download_path_mf4)
        os.remove(download_path_config)
        logging.info(f'os Arquivos:{csv_path}, {download_path_config} e {download_path_mf4} foram removidos com sucesso do {tmpFilePath}')
    except Exception as err:
        logging.error(f'Ocorreu um erro na limpeza do {tmpFilePath}: {err}')
        return
    files = glob.glob(tmpFilePath + '/*')
    logging.info(f'Os arquivos atuais da pasta {tmpFilePath}: {files}')
    return None
