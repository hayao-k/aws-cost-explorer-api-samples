from logging import getLogger, INFO
import os
import datetime
import boto3
import pandas
from botocore.exceptions import ClientError

logger = getLogger()
logger.setLevel(INFO)

def upload_s3(output, key, bucket):
    try:
        s3_resource = boto3.resource('s3')
        s3_bucket = s3_resource.Bucket(bucket)
        s3_bucket.upload_file(output, key, ExtraArgs={'ACL': 'bucket-owner-full-control'})
    except ClientError as err:
        logger.error(err.response['Error']['Message'])
        raise

def get_ou_ids(org, parent_id):
    ou_ids = []

    try:
        paginator = org.get_paginator('list_children')
        iterator = paginator.paginate(
            ParentId=parent_id,
            ChildType='ORGANIZATIONAL_UNIT'
        )
        for page in iterator:
            for ou in page['Children']:
                ou_ids.append(ou['Id'])
                ou_ids.extend(get_ou_ids(org, ou['Id']))
    except ClientError as err:
        logger.error(err.response['Error']['Message'])
        raise
    else:
        return ou_ids

def list_accounts():
    org = boto3.client('organizations')
    root_id = 'r-xxxx'
    ou_id_list = [root_id]
    ou_id_list.extend(get_ou_ids(org, root_id))
    accounts = []

    try:
        for ou_id in ou_id_list:
            paginator = org.get_paginator('list_accounts_for_parent')
            page_iterator = paginator.paginate(ParentId=ou_id)
            for page in page_iterator:
                for account in page['Accounts']:
                    item = [
                        account['Id'],
                        account['Name'],
                    ]
                    accounts.append(item)
    except ClientError as err:
        logger.error(err.response['Error']['Message'])
        raise
    else:
        return accounts

def get_cost_json(start, end):
    ce = boto3.client('ce')
    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': start,
            'End' :  end,
        },
        Granularity='DAILY',
        Metrics=[
            'NetUnblendedCost'
        ],
        GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'LINKED_ACCOUNT'
            }
        ]
    )
    return response['ResultsByTime']

def lambda_handler(event, context):
    today = datetime.date.today()
    start = today.replace(day=1).strftime('%Y-%m-%d')
    end = today.strftime('%Y-%m-%d')
    key = 'daily-cost-' + today.strftime('%Y-%m') + '.csv'
    output_file = '/tmp/output.csv'
    bucket = os.environ['BUCKET']
    account_list = pandas.DataFrame(list_accounts(), columns=['Account Id', 'Account Name'])
    daily_cost_list = get_cost_json(start, end)

    merged_cost = pandas.DataFrame(
        index=[],
        columns=['Account Id']
    )

    for index, item in enumerate(daily_cost_list):
        normalized_json = pandas.json_normalize(item['Groups'])
        split_keys = pandas.DataFrame(
            normalized_json['Keys'].tolist(),
            columns=['Account Id']
        )
        cost = pandas.concat(
            [split_keys, normalized_json['Metrics.NetUnblendedCost.Amount']],
            axis=1
        )
        renamed_cost = cost.rename(
            columns={'Metrics.NetUnblendedCost.Amount': item['TimePeriod']['Start']}
        )
        merged_cost = pandas.merge(merged_cost, renamed_cost, on='Account Id', how='outer')

    daily_cost = pandas.merge(account_list, merged_cost, on='Account Id', how='right')
    daily_cost.to_csv(output_file, index=False)
    upload_s3(output_file, key, bucket)
