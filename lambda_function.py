# -*- coding: utf-8 -*-
import boto3
import json
import datetime
import os
import logging
from base64 import b64decode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

ENCRYPTED_HOOK_URL = os.environ['kmsEncryptedHookUrl']
SLACK_CHANNEL = os.environ['slackChannel']
HOOK_URL = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_HOOK_URL))['Plaintext'].decode('utf-8')

logs = boto3.client('logs')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#何分前からのメトリクスを検索対象とするか
TIME_FROM_MIN=10

#Lambdaファンクションのファンクション名のリストを返す。
def get_function_names(region_name):
    lmd = boto3.client('lambda',region_name=region_name)
    response = lmd.list_functions()
    function_names = [d['FunctionName'] for d in response['Functions']]
    return function_names


def lambda_handler(event, context):
    message = event['Records'][0]['Sns']['Message']
    message = json.loads(message)

    alarm_name  = message['AlarmName']
    new_state   = message['NewStateValue']
    metric      = message['Trigger']['MetricName']
    namespace   = message['Trigger']['Namespace']

    region = 'ap-northeast-1'
    cloudwatch = boto3.client('cloudwatch',region_name = region)

    #Lambdaファンクション名のリストを取得する
    function_names = get_function_names(region)

    #TIME_FROM_MIN分前からStateChangeTimeの一分後までのメトリクスを対象とする
    timeto = datetime.datetime.strptime(message['StateChangeTime'][:19] ,'%Y-%m-%dT%H:%M:%S') + datetime.timedelta(minutes=1)
    timefrom = timeto - datetime.timedelta(minutes=TIME_FROM_MIN)

    #FunctionNameごとに直近時間帯のメトリクスデータを取得
    errorpoints = []
    for function_name in function_names:
        if str(function_name).startswith('cbr_dev_'):
                
            datapoints = cloudwatch.get_metric_statistics(
                Namespace  = namespace,
                MetricName = metric,
                StartTime  = timefrom,
                EndTime    = timeto,
                Period     = 300,
                Dimensions = [
                    {
                        'Name': 'FunctionName',
                        'Value': function_name
                    }
                ],
                Statistics = ['Sum']
            )
            #SUM > 0の場合はエラーが発生している。
            #エラー発生のメトリクスデータを抽出し、errorpointsリストに追加する。
            errorpoint = list(filter(lambda x: x['Sum'] > 0, datapoints['Datapoints']))

            if len(errorpoint) > 0:
                errorpoints.append({"FunctionName": function_name, "Datapoints": errorpoint})
            
            metricfilters = logs.describe_metric_filters(
                metricName = metric,
                metricNamespace = namespace
            )

    #Timestampの逆順で（＝新しい順に）ソート
    sortedList = sorted(errorpoints, key=lambda x:x['Datapoints'][0]['Timestamp'],reverse=True)
    
    #件名:直近でエラーになったFunctionNameを件名に含める
    title = alarm_name + new_state + sortedList[0]['FunctionName']

    #本文：直近時間帯でエラーになったすべてのFunctionNameとエラー件数を本文に含める
    for e in sortedList:
        #タイムスタンプは日本時刻に変換する
        date = e['Datapoints'][0]['Timestamp'] + datetime.timedelta(hours=9)
        message = str(date)[:19] + 'に `' + str(e['FunctionName']) + '` でエラーが' + str(int(e['Datapoints'][0]['Sum'])) + '回発生しました'

    #slack post
    try:
        slack_message = {
            'channel': SLACK_CHANNEL,
            'text': message
        }
        req = Request(HOOK_URL, json.dumps(slack_message).encode('utf-8'))
        response = urlopen(req)
        response.read()
        logger.info("Message posted to %s", slack_message['channel'])

    except HTTPError as e:
        logger.error("Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        logger.error("Server connection failed: %s", e.reason)
