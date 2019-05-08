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

if os.environ.get('http_proxy') is not None:
    del os.environ['http_proxy']
if os.environ.get('https_proxy') is not None:
    del os.environ['https_proxy']

cfg = util.load_cfg('celcom_contract_config.yml')
id = cfg['id']

# NOTE: update this!
date = '2018-09-15'
DRUID = 'http://localhost:8081'
logging.getLogger().setLevel(logging.INFO)


non_prefixed_path = f'/LYNX/HI/lynxkite_hi_data/exports/druid_13_ingre/happiness_celcom_contract_2018-09-15'

columns = ['act_tenure', 'active_days_category', 'active_days_count', 'active_days_score', 'age_cat', 'campaign_and_promotion_experience_category', 'campaign_and_promotion_experience_count', 'campaign_and_promotion_experience_score', 'campaign_response_category', 'campaign_response_count', 'campaign_response_score', 'cobp_category', 'cobp_count', 'cobp_score', 'contract_type', 'cust_segment', 'customer_service_experience_category', 'customer_service_experience_count', 'customer_service_experience_score', 'data_usage_category', 'data_usage_count', 'data_usage_score', 'date', 'device_type', 'district', 'gender', 'ingre_bill_amt_category', 'ingre_bill_amt_count', 'ingre_bill_amt_score', 'ingre_device_category', 'ingre_device_count', 'ingre_device_score', 'ingre_incoming_outgoing_category', 'ingre_incoming_outgoing_count', 'ingre_incoming_outgoing_score', 'ingre_social_category', 'ingre_social_count', 'ingre_social_score', 'ingre_throughput_category', 'ingre_throughput_count', 'ingre_throughput_score', 'ingre_url_category', 'ingre_url_count', 'ingre_url_score', 'ingre_video_category', 'ingre_video_count', 'ingre_video_score', 'micro_segmentation', 'mobile_contract', 'month_to_expiry', 'network_experience_category', 'network_experience_count', 'network_experience_score', 'overall_hi_category', 'overall_hi_score', 'person_id', 'service_usage_category', 'service_usage_count', 'service_usage_score', 'social_experience_category', 'social_experience_count', 'social_experience_score', 'spending_cat', 'sub_tenure', 'vas_category', 'vas_count', 'vas_score']

druid.load_file(DRUID, f'happiness_{id}', non_prefixed_path, columns, datetime.strptime(date, '%Y-%m-%d'), **segmentation.druid_config(cfg))