from __future__ import print_function

import json
import urllib
import boto3
import datetime

def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    ############ S3

    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    print('bucket: ' + bucket)

    original_key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    print('original_key: ' + original_key)

    new_key = original_key.strip().lower().replace(' ', '_').replace('-', '').replace('__', '_')
    print('new_key: ' + new_key)

    if ( new_key != original_key):
        print('Starting copy...')
        copy_source = {'Bucket':bucket, 'Key':original_key}
        response = s3.copy_object(Bucket=bucket, Key=new_key, CopySource=copy_source)
        # s3.delete_object(Bucket=bucket, Key=original_key)
        print('Renamed object')


    ############ Elastic Transcoder

    transcoder = boto3.client('elastictranscoder')
    # pipeline_id = '1498452009683-51okq7' #read_pipeline('BucketHead Standard Prioriy')
    # new pipeline id ties to cloudformation stack:
    pipeline_id = '1500336162564-c39ztt'
    print('Sending to pipeline: ' + pipeline_id)

    # transmogrify new_key into transcribed version, with suffix handled
    new_key_split = new_key.split('.') # key and and extension name (extension in [1])
    new_key_zero, new_key_ext = new_key_split

    new_key_480p = new_key_zero + "_480p." + new_key_ext
    print("new_key_480p: " + new_key_480p)

    print("about to transcode...")
    output = transcoder.create_job(
        PipelineId=pipeline_id,
        Input={
            'Key': new_key,
            'FrameRate': 'auto',
            'Resolution': 'auto',
            'AspectRatio': 'auto',
            'Interlaced': 'auto',
            'Container' : 'auto'
        },
        Outputs=[{
                'Key': new_key_480p, # need to get last part of s3 key
                'PresetId': '1351620000001-100020',
                'ThumbnailPattern': new_key[:-4] + '_{count}'
            }
        ]
    )
    print('Called transcoder.create_job')

    ############ SES
    # ses = boto3.client('ses')
    # from: https://github.com/thigley986/Lambda-AWS-SES-Send-Email/blob/master/SendEmail.py
    # email_from = 'john@fnnny.com'
    # email_to = 'john@fnnny.com'
    # emaiL_subject = 'Lamba ran after upload!'
    # email_body = 'https://' + bucket + '.s3.amazonaws.com/' + new_key
    # print('about to send email')
    # response = ses.send_email(
    #    Source = email_from,
    #    Destination={ 'ToAddresses': [ email_to, ] },
    #    Message={ 'Subject': { 'Data': emaiL_subject }, 'Body': { 'Text': { 'Data': email_body } } }
    #)

    # print('No GET permissions. Instead send email that an object was uploaded')
    # response = s3.get_object(Bucket=bucket, Key=key)
    # print("CONTENT TYPE: " + response['ContentType'])
    # return response['ContentType']
    # except Exception as e:
    # print(e)
    # print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
    # raise e
