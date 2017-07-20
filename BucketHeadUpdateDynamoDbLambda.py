import json
import urllib
import boto3

def lambda_handler(event, context):
    
    ############ SES
    ses = boto3.client('ses')
    # from: https://github.com/thigley986/Lambda-AWS-SES-Send-Email/blob/master/SendEmail.py
    email_from = 'john@fnnny.com'
    email_to = 'john@fnnny.com'
    email_subject = 'Lamba ran after transcode!'
    email_body = json.dumps(event, indent=2)
    
    response = ses.send_email(
       Source = email_from,
       Destination={ 'ToAddresses': [ email_to, ] },
       Message={
           'Subject': { 'Data': email_subject },
           'Body': {
               'Text': {
                    #'Data': 'job_id: ' + job_id + '; created_at: ' + created_at
                    'Data': email_body
                    #'Data': msg
               }
           }
       }
    )
    
    created_at = event['Records'][0]['Sns']['Timestamp']
    msg = json.loads(event['Records'][0]['Sns']['Message'])
    #print('msg: ' + msg)
    
    videoId = msg['jobId']
    print('videoId: ' + videoId)
    
    original_input = msg['input']['key']
    print('original_input: ' + original_input)
    
    print('msg[outputs][0][key]: ' + msg['outputs'][0]['key'])
    print ('the type: ' + type( msg['outputs'][0]['key'] ).__name__)
    duration = msg['outputs'][0]['duration']
    trans_480_key = msg['outputs'][0]['key']
    
    thumbnail = original_input[:-4] + '_00001.png'
    
    
    
    
    print('trans_480_key: ' + trans_480_key)
    # 
    
    # Pass-thru metadata is now supported for jobs:
    # https://aws.amazon.com/about-aws/whats-new/2014/12/10/amazon-elastic-transcoder-now-supports-metadata-pass-through-for-jobs/
    # Can this be used to pass through stuff like user id, game id, etc?
    
    ############ DynamoDB
    print('Starting dynamo db input...')
    dynamodb = boto3.client('dynamodb')
    
    # response = dynamodb.scan(
    #     TableName='bhead-thumb-count'
    # )
    # id = response['Items'][0]['id']
    # count = response['Items'][0]['cnt']['N']
    #
    # print('count:')
    # print( json.dumps(count, indent=2) )
    #
    # updated_count = int(count) + 1
    # print('updated_count is: ' + str(updated_count))
    #
    # padded_updated_count = '{:05d}'.format(updated_count)
    # print('padded_updated_count is: ' + padded_updated_count)
    #
    # response = dynamodb.update_item(
    #     TableName='bhead-thumb-count',
    #     Key={'id': {'S': '1'}},
    #     UpdateExpression="set cnt = :c",
    #     ExpressionAttributeValues={
    #         ':c': {'N': str(updated_count)}
    #     }
    # )
    
    # Brittle as fuck, would like to be able to put this in a transaction
    
    # 'ThumbnailPattern': new_key[:-4] + '_{count}'
    
    response = dynamodb.put_item(
        TableName='BucketHead-Videos-H2YWUKASUWGO',
        Item={
            'VideoId': {'S': videoId},
            'created_at': {'S': created_at},
            'original_input': {'S': original_input},
            '480p_transcoded': {'S': trans_480_key},
            'thumbnail': {'S': thumbnail},
            'duration': {'S': str(duration)}
        }
    )
    