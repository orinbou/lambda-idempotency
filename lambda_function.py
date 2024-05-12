import json
import os
import datetime
import boto3
import platform
import logging
from time import sleep
from dateutil import tz
from botocore.exceptions import ClientError

# ログレベル設定
logger = logging.getLogger()
logLevelTable={'DEBUG':logging.DEBUG,'INFO':logging.INFO,'WARNING':logging.WARNING,'ERROR':logging.ERROR,'CRITICAL':logging.CRITICAL}
if 'LOG_LEVEL' in os.environ and os.getenv('LOG_LEVEL') in logLevelTable :
    logLevel = logLevelTable[os.getenv('LOG_LEVEL')]
else:
    logLevel=logging.WARNING # デフォルトは警告以上をログ出力する
logger.setLevel(logLevel)

# DynamoDBテーブル名
TABLE_NAME = 'lambda-idempotency-status'

dynamo = boto3.resource('dynamodb')
dynamo_table = dynamo.Table(TABLE_NAME)

# 実行ステータス定義
STATUS_RUNNING = 'running'
STATUS_SUCCESS = 'success'
STATUS_ERROR = 'error'

# TTL（Time to Live）定義（秒）
TTL_SECONDS_VALUE = 24 * 60 * 60

# UNIXエポック時間形式（秒）TTL値取得
def get_ttl_unix_epoch_time():
    return int((datetime.datetime.now(tz=tz.gettz('Asia/Tokyo')) + datetime.timedelta(seconds=TTL_SECONDS_VALUE)).timestamp())

# 開始ステータス登録
def write_start_status(status):
    try:
        status['Status'] = STATUS_RUNNING
        status['CreatedAt'] = datetime.datetime.now(tz=tz.gettz('Asia/Tokyo')).strftime('%Y-%m-%d %H:%M:%S')
        status['ExpireTTL'] = get_ttl_unix_epoch_time()
        status['Histories'] = [
            {
                'CreatedAt': status['CreatedAt'],
                'Status': status['Status'],
                'Detail': status['Detail']
            }
        ]
        # DB登録
        dynamo_table.put_item(
            Item=status,
            ConditionExpression='attribute_not_exists(JobId) AND attribute_not_exists(ObjKey)'
        )
        return True
    except ClientError as e:
        #logger.error(f'error: {e}')
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return False
        else:
            raise

# 終了ステータス登録
def write_final_status(status):
    try:
        status['CreatedAt'] = datetime.datetime.now(tz=tz.gettz('Asia/Tokyo')).strftime('%Y-%m-%d %H:%M:%S')
        status['ExpireTTL'] = get_ttl_unix_epoch_time()
        # DB更新
        dynamo_table.update_item(
            Key={
                'JobId': status['JobId'],
                'ObjKey': status['ObjKey']
            },
            ExpressionAttributeNames={
                '#time': 'CreatedAt',
                '#stat': 'Status',
                '#dtls': 'Detail',
                '#hist': 'Histories'
            },
            ExpressionAttributeValues={
                ':upd_time': status['CreatedAt'],
                ':upd_stat': status['Status'],
                ':upd_dtls': status['Detail'],
                ':add_hist': [
                    {
                        'CreatedAt': status['CreatedAt'],
                        'Status': status['Status'],
                        'Detail': status['Detail']
                    }
                ]
            },
            UpdateExpression='SET #time = :upd_time, #stat = :upd_stat, #dtls = :upd_dtls, #hist = list_append(#hist, :add_hist)'
        )
    except Exception as e:
        logger.error(f'error: {e}')
        raise e

# エントリーポイント
def lambda_handler(event, context):
    logger.info(f'start')

    # 引数確認
    logger.info(f'event : {event}')

    # 動作環境確認
    logger.info(f'python_version : {platform.python_version()}')
    logger.info(f'boto3.version : {boto3.__version__}')

    # ステータス初期化
    job_status = {
        'JobId': event['JobId'],
        'ObjKey': event['ObjKey'],
        'RequestID': context.aws_request_id,
        'Function': context.function_name,
        'Status': '',
        'Detail': '',
        'Histories': [],
        'CreatedAt' : '',
    }
    logger.info(f'job_status : {job_status}')

    try:
        # 開始ステータス登録
        is_start_succeed = write_start_status(job_status)
        if is_start_succeed:
            # メインの処理
            logger.info(f'メインの処理【開始】')
            sleep(1)
            #raise RuntimeError('An unexpected error has occurred.')
            logger.info(f'メインの処理【終了】')

        logger.info(f'success')
        job_status['Status'] = STATUS_SUCCESS

    except Exception as e:
        logger.error(f'error: {e}')
        job_status['Status'] = STATUS_ERROR
        job_status['Detail'] = format(e)

    finally:
        logger.info(f'finally')
        if is_start_succeed:
            # 最終ステータス登録
            write_final_status(job_status)
            # 最終ステータス成功以外は実行エラー
            if job_status['Status'] != STATUS_SUCCESS:
                logger.error(f'{job_status["Detail"]}')
                raise RuntimeError('Job lambda was not successful.')
        else:
            # 多重起動の場合は何もせず即終了する
            logger.warn(f'lambdaが多重起動したため処理を終了しました。')

        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
