#!/usr/bin/env /home/lynx_hi/env/miniconda3/envs/airflow-conda3-env/bin/python
import lynx.kite
import daily_data
import druid
import logging
import os
import segmentation
import util
from datetime import datetime
import subprocess

lk = lynx.kite.LynxKite(
    username="admin",
    password="kite@celcom",
    address="https://localhost:3201",
    certfile="/home/lynx_hi/lynx_hi_ecosystem/config/ssl/localhost.self-signed.cert.pub"
)

cfg = util.load_cfg('celcom_contract_config.yml')
# NOTE: update this!
date = '2018-09-15'
DRUID = 'http://localhost:8081'
logging.getLogger().setLevel(logging.INFO)
def load_state(state, name, **kwargs):
    non_prefixed_path = f'/LYNX/HI/lynxkite_hi_data/exports/druid_13_ingre/{name}_{date}'
    prefixed_path = f'EMPTY${non_prefixed_path}' # for use by lk
    print(f'druid_path: {non_prefixed_path}')
    # remove existing data if there is any
    subprocess.run(['hdfs', 'dfs', '-rm', '-r', f'{non_prefixed_path}'])
    try:
      state.exportToCSVNow(path=prefixed_path, header=False)
    except:
      pass
    subprocess.run(['hdfs', 'dfs', '-rm', '-r', f'{non_prefixed_path}-parquet'])
    try:
      state.exportToParquetNow(path=f'{prefixed_path}-parquet')
    except:
      pass
    druid.load_file(DRUID, name, non_prefixed_path, state.columns(), datetime.strptime(date, '%Y-%m-%d'), **kwargs)


id = cfg['id']
dt_partition = datetime.strptime(date, '%Y-%m-%d').strftime('YEAR=%Y/MONTH=%-m/DAY=%d')
t = lk.importParquetNow(filename=f"EMPTY$/LYNX/HI/PIPELINE/DATA_WAREHOUSE/UPDATE/CHI_OUTPUT_CONTRACT/{dt_partition}")
r = segmentation.pipeline(cfg, ingredients=t, filters=t, date=date)
hi_snapshot_name = f'hi_13_ingre/{id}/{date}'
daily_data_snapshot_name = f'daily_data_13_ingre/{id}/{date}'
lk.remove_name(hi_snapshot_name, force=True)
lk.remove_name(daily_data_snapshot_name, force=True)
r['hi'].save_snapshot(hi_snapshot_name)
r['daily_data'].save_snapshot(daily_data_snapshot_name)

print('id:', id)
load_state(r['hi'], f'happiness_{id}', **segmentation.druid_config(cfg))
load_state(r['daily_data'], f'daily_data_{id}', **daily_data.druid_config(cfg))