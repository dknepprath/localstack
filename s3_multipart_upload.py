import boto3, os, json
from boto3 import client
import time



lambda_client = client(
    service_name='lambda',
    endpoint_url='http://localhost:4574',
    region_name='',
    aws_access_key_id='',
    aws_secret_access_key='')


def create_lambda_function():
    env_variables = dict() # Environment Variables
    with open('lambda.zip', 'rb') as f:
        zipped_code = f.read()

    try:
        lambda_client.create_function(
            FunctionName='myLambdaFunction',
            Runtime='python2.7',
            Role='r1',
            Handler='lambda.handler',
            Code=dict(ZipFile=zipped_code),
            Timeout=300, # Maximum allowable timeout
            Environment=dict(Variables=env_variables),
        )
    except:
        lambda_client.update_function_code(
            FunctionName='myLambdaFunction',
            ZipFile=zipped_code)


def invoke_lambda():
    print("invoking")
    response = lambda_client.invoke(
        FunctionName='myLambdaFunction',
        InvocationType='RequestResponse',
        LogType='Tail')

    print(response["Payload"]._raw_stream.data)

# THIS IS NOT HOW S3 EVENT NOTIFICATION WORK!
# You configure the event nofification in S3 using put_bucket_notification_configuration
# Kinesis, DynamoDB, and Amazon SQS queues do poll-based event
# S3 is a push model
def create_event_source_mapping():
    lambda_client.create_event_source_mapping(
        FunctionName='myLambdaFunction',
        EventSourceArn='arn:aws:s3:::test_event_data_bucket',
        Enabled=True
    )

s3_client = client(
    service_name='s3',
    endpoint_url='http://localhost:4572',
    region_name='',
    aws_access_key_id='',
    aws_secret_access_key='')

test_bucket = "test_event_data_bucket"

def put_bucket_notification_configuration():
    response = s3_client.put_bucket_notification_configuration(
        Bucket=test_bucket,
        NotificationConfiguration= {'LambdaFunctionConfigurations':[{'LambdaFunctionArn': 'arn:aws:lambda:us-east-1:000000000000:function:myLambdaFunction',
                                                                     'Events': ['s3:ObjectCreated:*']}]})
    print(response)

def create_bucket():
    response = s3_client.create_bucket(Bucket=test_bucket)

def put_object_in_bucket():
    for file in os.listdir("resources"):
        path = os.path.join("resources", file)
        # with open(path, 'r') as data:
        print("put object")
        s3_client.put_object(Bucket=test_bucket,
                             Key='pre-deployment-validation/key',
                             Body=file)


    object = s3_client.get_object(Bucket=test_bucket,
                         Key='pre-deployment-validation/key')
    print(object)

    part = 1

    response = s3_client.list_objects_v2(Bucket=test_bucket,
                              Prefix='pre-deployment-validation')

    print(response)


def multipart_upload():
    part = 1
    multipart_key = "multipart_object_new"
    multipart_upload_parts = []
    multipart_upload_dict = s3_client.create_multipart_upload(Bucket=test_bucket,
                                                              Key=multipart_key)

    for file in os.listdir("resources"):
        path = os.path.join("resources", file)
        print('Uploading file ' + path + ' as part ' + str(part))
        with open(path, 'r') as data:
        # try:
            response = s3_client.upload_part(Bucket=test_bucket,
                                  Body=data.read(),
                                  Key=multipart_key,
                                  PartNumber=part,
                                  UploadId=multipart_upload_dict['UploadId'])
            print(response)

            multipart_upload_parts.append({'ETag': response['ETag'], 'PartNumber': part})

            part = part + 1
        # except Exception as ex:
        # print(str(ex))

    # TODO grab the ETag valu and PartNumber and store in an array for completing the upload

    # ABORT UPLOAD
    reponse = s3_client.abort_multipart_upload(Bucket=test_bucket,
                                               Key=multipart_key,
                                               UploadId=multipart_upload_dict['UploadId'])


    # COMPLETE UPLOAD
    # response = s3_client.complete_multipart_upload(Bucket=test_bucket,
    #                                     Key=multipart_key,
    #                                     MultipartUpload={'Parts': multipart_upload_parts},
    #                                     UploadId=multipart_upload_dict['UploadId'])
    print(response)






if __name__ == "__main__":
    create_lambda_function()
    # create_event_source_mapping()
    create_bucket()
    # multipart_upload()
    # put_object_in_bucket()
    put_bucket_notification_configuration()
    invoke_lambda()

