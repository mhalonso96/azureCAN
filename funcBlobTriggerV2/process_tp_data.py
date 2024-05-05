import etncanedge_files, os
from .utils import setup_fs, load_dbc_files, restructure_data, ProcessData, MultiFrameDecoder, add_custom_sig
from datetime import datetime, timezone
import pandas as pd
# from asammdf import MDF
from pathlib import Path
import json
import numpy as np
from pathlib import Path
import pytz

def read_file_MF4(dir, config):
    files_MF4 = []
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.endswith(".MF4"):
                path = file
                files_MF4.append(path)
    check = files_MF4[0].replace('%2F', '/').split('/')
    if config[4][0]['device'] == check[0]:
        return files_MF4
    
    else:
        raise (f'Verique o arquivo de config.json --- {check[0]} --- {config}')

def readConfig(URL_config):
    with open (URL_config, 'r') as file:
        lista = json.load(file)
    print(f'Iniciando a leitura do arquivo de configuracao')
    filter_signals = [filter['signal'] for filter in lista['filter_signals']]
    filter_gps = [filter['signal'] for filter in lista['filter_gps_signals']]
    config_s3 = [config for config in lista['config_s3']]
    date_time = [date for date in lista['datetime']]
    infos = [info for info in lista['info']]
    dbc = [dbc for dbc in lista['dbc']]
    print(f'Leitura do arquivo de config completada')
    return [filter_signals, filter_gps, config_s3, date_time, infos, dbc]


def processingTP(blob_name, path, dbc_paths, tp_type, filter_signals, filter_gps, config_s3, date_time, info, name_file):
    vehicle = info[1]['vehicle']
    device = info[0]['device']
    print(f"Processamento iniciado", info[0]['device'])
    start = datetime(year=date_time[0]['start'], day=1, month=1, hour=0, tzinfo=timezone.utc)
    stop = datetime(year=date_time[1]['stop'], day=1, month=1,  hour=0, tzinfo=timezone.utc)
    fuso_horario_brasilia = pytz.timezone('America/Sao_Paulo')
    current_date = datetime.now(fuso_horario_brasilia).strftime("%d%m%Y_%H_%M_%S")
    
    fs = setup_fs(
                s3=config_s3[0]['s3'], 
                key=config_s3[1]['key'],
                secret=config_s3[2]['secret'], 
                endpoint=config_s3[3]['endpoint'], 
                region=config_s3[4]['region'], 
                passwords=config_s3[5]['pw'])
    
    db_list = load_dbc_files(dbc_paths)
    print(f'load_dbc_files completed{blob_name} e {path}')
    log_files = etncanedge_files.get_log_files(fs, devices=blob_name, path=path, start_date=start, stop_date=stop)
    print(f'get_log_files completed, {log_files}')
    proc = ProcessData(fs, db_list)
    print(f'ProcessData completed')
    df_phys_all = []

    for log_file in log_files:
        
        df_raw, device_id = proc.get_raw_data(log_file)            
 
        # replace transport protocol sequences with single frames
        tp = MultiFrameDecoder(tp_type)
        df_raw = tp.combine_tp_frames(df_raw)

        # extract physical values as normal, but add tp_type
        df_phys = proc.extract_phys(df_raw)
        proc.print_log_summary(device_id, log_file, df_phys)
        df_phys_all.append(df_phys)

                       
    df_phys_all = pd.concat(df_phys_all,ignore_index=False).sort_index()
    print("Finished saving CSV output for devices:", info[0]['device'])
        #   # add a custom signal
        # def ratio(s1, s2):
        #     return s2 / s1 if s1 else np.nan
        # print(df_phys_all)
        # df_phys_all = add_custom_sig(df_phys_all, "WheelBasedVehicleSpeed", "EngineSpeed", ratio, "RatioRpmSpeed")
    
    df_phys_join = restructure_data(df_phys=df_phys_all, res="1s", filter_signals=filter_signals, filter_gps=filter_gps)
    print(f'restructure_data completed')
    
    
    csv = f'{path}/{vehicle}_{device}_{name_file}_{current_date}.csv'
    print(f'\nDiretorio do arquivo csv: {csv} \n')
    df_phys_join.to_csv(csv)
    print("\nConcatenated DBC decoded data:\n", df_phys_join)
    return csv



