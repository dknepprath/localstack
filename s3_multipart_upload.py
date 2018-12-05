import boto3, os, json
from boto3 import client
import time



# lambda_client = boto3.client('lambda')

s3_client = client(
    service_name='s3',
    endpoint_url='http://localhost:4572',
    region_name='',
    aws_access_key_id='',
    aws_secret_access_key='')

test_bucket = "test_event_data_bucket"

def create_bucket():
    s3_client.create_bucket(Bucket=test_bucket)


    for file in os.listdir("resources"):
        path = os.path.join("resources", file)
        # with open(path, 'r') as data:
        print("put object")
        s3_client.put_object(Bucket=test_bucket,
                             Key='pre-deployment-validation/key',
                             Body=file)


    object = s3_client.get_object(Bucket=test_bucket,
                         Key='pre-deployment-validation')
    print(object)

    part = 1

    response = s3_client.list_objects_v2(Bucket=test_bucket,
                              Prefix='pre-deployment-validation')

    print(response)


def multipart_upload():
    multipart_key = "multipart_object"
    multipart_upload_parts = []
    multipart_upload_dict = s3_client.create_multipart_upload(Bucket=test_bucket,
                                                              Key=multipart_key)

    for file in os.listdir("resources"):
        path = os.path.join("resources", file)
        print('Uploading file ' + path + ' as part ' + str(part))
        part = part + 1
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

    response = s3_client.complete_multipart_upload(Bucket=test_bucket,
                                        Key=multipart_key,
                                        MultipartUpload={'Parts': multipart_upload_parts},
                                        UploadId=multipart_upload_dict['UploadId'])
    print(response)






