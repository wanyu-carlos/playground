#!/usr/bin/env python3
'''For loading data into Druid.'''

import datetime as dt
import json
import logging
import lynx
import requests
import time
import yaml
import util
import shutil

SLEEP_SECONDS = 5

def _base_spec(filename, datasource, date):
  spec = yaml.load('''
type: index_hadoop
spec:
  ioConfig:
    type: hadoop
    inputSpec:
      type: static
      paths: REPLACE_ME
  tuningConfig:
    type: hadoop
    partitionsSpec: REPLACE_ME
  dataSchema:
    dataSource: REPLACE_ME
    granularitySpec:
      intervals : [REPLACE_ME]
    parser:
      parseSpec:
        format: csv
        columns: []
        dimensionsSpec:
          dimensions: []
        timestampSpec:
          format: auto
          column: date
    metricsSpec:
      - name: count
        type: count
  ''')
  spec['spec']['ioConfig']['inputSpec']['paths'] = filename
  spec['spec']['dataSchema']['granularitySpec']['intervals'] = [_interval(date)]
  spec['spec']['dataSchema']['dataSource'] = datasource
  return spec

def _interval(date):
  '''Returns the interval corresponding to the given day.'''
  tomorrow = date + dt.timedelta(days=1)
  return f'{date:%Y-%m-%d}/{tomorrow:%Y-%m-%d}'


def get_datasources(druid_address):
  return requests.get(f'{druid_address}/druid/coordinator/v1/datasources').json()


def get_segments(druid_address, datasource):
  return requests.get(
      f'{druid_address}/druid/coordinator/v1/metadata/datasources/{datasource}/segments?full'
  ).json()

def _add_segments_if_exists(druid_address, spec):
  datasource = spec['spec']['dataSchema']['dataSource']
  datasources = get_datasources(druid_address)
  if datasource in datasources:
    logging.info('DRUID: %s exists, fetching segments', datasource)
    segments = get_segments(druid_address, datasource)
    intervals = [s['interval'] for s in segments]
    filespec = spec['spec']['ioConfig']['inputSpec']
    spec['spec']['ioConfig']['inputSpec'] = dict(
        type='multi',
        children=[filespec, dict(
            type='dataSource',
            ingestionSpec=dict(
                dataSource=datasource,
                intervals=intervals,
                segments=segments))])


def _start_task(druid_address, spec):
  r = requests.post(
      druid_address + '/druid/indexer/v1/task',
      data=json.dumps(spec, indent=2),
      headers={'Content-Type': 'application/json'})
  r.raise_for_status()
  return r.json()['task']


def _await_task(druid_address, task):
  while True:
    time.sleep(SLEEP_SECONDS)
    r = requests.get(f'{druid_address}/druid/indexer/v1/task/{task}/status')
    print(json.dumps(r.json(), indent=2))
    status = r.json()['status']['status']
    if status == 'SUCCESS':
      return
    elif status == 'RUNNING':
      pass
    else:
      raise Exception('Druid task failed.')

def _is_loaded(druid_address, datasource, date, num_shards):
  '''Asks the Broker about the presence of the shards.'''
  interval = _interval(date)
  broker_address = druid_address.replace('8081', '8082')
  segments = requests.get(
      f'{broker_address}/druid/v2/datasources/{datasource}/candidates?intervals={interval}').json()
  return len(segments) == num_shards and all(s['locations'] for s in segments)

def load_state(druid_address, datasource, table_state, date, dimensions, metrics, num_shards=8):
  '''Load a table into Druid.'''
  prefixed_path = f'DATA$/exports/druid/{datasource}_{date}'
  filename = table_state.box.lk.get_prefixed_path(prefixed_path).resolved
  table_state.exportToCSVNow(path=prefixed_path, header=False)
  load_file(
      druid_address, datasource, filename, table_state.columns(), date, dimensions, metrics,
      num_shards)

def load_file(
        druid_address, datasource, filename, columns, date, dimensions, metrics, num_shards=8):
  '''Load a CSV file into Druid.'''
  logging.info('DRUID: Loading %s into %s', filename, druid_address)
  spec = _base_spec(filename, datasource, date)
  spec['spec']['tuningConfig']['partitionsSpec'] = dict(type='hashed', numShards=num_shards)
  pspec = spec['spec']['dataSchema']['parser']['parseSpec']
  pspec['columns'] = columns
  pspec['dimensionsSpec']['dimensions'] = dimensions
  mspec = spec['spec']['dataSchema']['metricsSpec']
  mspec.extend(metrics)
  _add_segments_if_exists(druid_address, spec)
  logging.info('DRUID: Starting task...')
  logging.debug('DRUID: Spec:\n%s', json.dumps(spec, indent=2))
  task = _start_task(druid_address, spec)
  logging.info('DRUID: Awaiting task %s...', task)
  _await_task(druid_address, task)
  logging.info('DRUID: Indexing done.')


def wait_until_ready(druid_address, datasource, date, num_shards):
  while True:
    if _is_loaded(druid_address, datasource, date, num_shards):
      return
    logging.info('DRUID: Waiting for segments to load...')
    time.sleep(SLEEP_SECONDS)

def delete(druid_address, datasource):
  '''Deletes the datasource if it exists.'''
  if datasource not in get_datasources(druid_address):
    return
  r = requests.delete(f'{druid_address}/druid/coordinator/v1/datasources/{datasource}')
  r.raise_for_status()
  while True:
    time.sleep(SLEEP_SECONDS)
    if datasource not in get_datasources(druid_address):
      return
    logging.info(f'DRUID: Waiting until {datasource} disappears...')