import os
import sys
sys.path.append(os.environ.get('PYTHON_PATH'))
import pytd
import pandas as pd
import shutil
os.system(f'{sys.executable} -m pip install paramiko=={os.environ.get("PARAMIKO_VERSION")}')
import paramiko
import io
from logger import logger as LOGGER
from datetime import datetime

# TreasureData設定
TD_API_KEY = os.environ.get('TD_API_KEY')
TD_API_SERVER = os.environ.get('TD_API_SERVER')
DATABASE = os.environ.get('DATABASE')

# SFTP設定
SFTP_HOSTNAME = os.environ.get('SFTP_HOSTNAME')
SFTP_PORT = os.environ.get('SFTP_PORT')
SFTP_USERNAME = os.environ.get('SFTP_USERNAME')
SFTP_PRIVATE_KEY_STR = os.environ.get('SFTP_PRIVATE_KEY_STR')
SFTP_PRIVATE_KEY_FILE_OBJ = io.StringIO(SFTP_PRIVATE_KEY_STR)
SFTP_PRIVATE_KEY_PKEY_CLS = paramiko.RSAKey(file_obj=SFTP_PRIVATE_KEY_FILE_OBJ)
SFTP_REMOTE_PATH_PREFIX = os.environ.get('SFTP_REMOTE_PATH_PREFIX')

# ファイル連携設定
COOP_NAME = os.environ.get('COOP_NAME')
WORK_DIR = os.environ.get('WORK_DIR')
SESSION_DATE = os.environ.get('SESSION_DATE')

# SQLファイル設定
PYTHON_PATH = os.environ.get('PYTHON_PATH')
SEGMENT_SQL_PATH = os.environ.get('SEGMENT_SQL_PATH')

def main():
    
    LOGGER.info(f'{datetime.now()}: {COOP_NAME}_連携処理 開始')
    
    LOGGER.info(f'{datetime.now()}: TreasureDataクライアント設定 開始')
    td_client = pytd.Client(apikey=TD_API_KEY, endpoint=TD_API_SERVER)
    LOGGER.info(f'{datetime.now()}: TreasureDataクライアント設定 終了')
    
    # 作業ディレクトリ作成
    if not os.path.exists(WORK_DIR):
        os.mkdir(WORK_DIR)
    
    # ファイル名設定
    file_name = f'extract_list_{SESSION_DATE}'
    query_path = f'{PYTHON_PATH}/{SEGMENT_SQL_PATH}'
    
    # 対象セグメントをcsv化(index行は除去)
    LOGGER.info(f'{datetime.now()}: 連携ファイル作成 開始')
    with open(query_path, 'r', encoding='utf-8') as f:
        query = f.read().format(database=DATABASE)
        
    res = td_client.query(query)
    df = pd.DataFrame(**res)
    df.to_csv(f'{WORK_DIR}/{file_name}.csv', index=False, header=True, quoting=2, quotechar='"')
    LOGGER.info(f'{datetime.now()}: 連携ファイル作成 終了')
    
    LOGGER.info(f'{datetime.now()}: SFTP接続 開始')
    # 接続するためのSSHクライアントの準備
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    # 接続先情報を設定して接続
    ssh_client.connect(
        hostname=SFTP_HOSTNAME,
        port=SFTP_PORT,
        username=SFTP_USERNAME,
        pkey=SFTP_PRIVATE_KEY_PKEY_CLS
    )
    sftp = ssh_client.open_sftp()
    
    # 連携ファイル配置
    sftp.put(f'{WORK_DIR}/{file_name}.csv',f'{SFTP_REMOTE_PATH_PREFIX}/{file_name}.csv')
    
    # セッション終了
    sftp.close()
    ssh_client.close()
    LOGGER.info(f'{datetime.now()}: SFTP接続 終了')
    
    # 作業ディレクトリ初期化
    shutil.rmtree(WORK_DIR)

    LOGGER.info(f'{datetime.now()}: {COOP_NAME}連携処理 終了')
