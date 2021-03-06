import json
import boto3
import os
from botocore.vendored import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

TABLE_NAME = 'yelp-restaurants'
SAMPLE_N = '5'
SEARCH_URL = 'https://search-dineinconcierge-2q6wr7ab7bn5uoqzpbclerolhe.us-west-2.es.amazonaws.com'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 'us-west-2', 'es', os.getenv('AWS_SESSION_TOKEN'))
sqs = boto3.resource('sqs',region_name='us-west-2')
es = Elasticsearch(SEARCH_URL, http_auth=awsauth, connection_class=RequestsHttpConnection)


def send_sms(number, message):
    # sns = boto3.client(
    #     'sns',
    #     aws_access_key_id=os.getenv('AWS_SERVER_PUBLIC_KEY'),
    #     aws_secret_access_key=os.getenv('AWS_SERVER_SECRET_KEY')
    # )
    send_sms = boto3.resource('sns', region_name='us-west-2')

    smsattrs = {
        'AWS.SNS.SMS.SenderID': {
            'DataType': 'String',
            'StringValue': 'TestSender'
        },
        'AWS.SNS.SMS.SMSType': {
            'DataType': 'String',
            'StringValue': 'Transactional'  # change to Transactional from Promotional for dev
        }
    }

    response = send_sms.publish(
        PhoneNumber=number,
        Message=message,
        MessageAttributes=smsattrs
    )
    print(number)
    print(response)
    print("The message is: ", message)


def search(cuisine):
    requestBody = {}
    requestBody['size'] = SAMPLE_N
    requestBody['query'] = {}
    requestBody['query']['bool'] = {}
    requestBody['query']['bool']['must'] = list([{
        'match': {
            'cuisine.title': cuisine
        }
    }])
    data = es.search(index="restaurants", body=requestBody)
    return data['hits']['hits']


def get_restaurant_data(ids):
    dynamodb = boto3.resource('dynamodb')
    payload = {}
    payload[TABLE_NAME] = {
        "Keys": [{'id': i} for i in ids]
    }
    response = dynamodb.batch_get_item(
        RequestItems=payload
    )

    res_data = response['Responses'][TABLE_NAME]
    ans = 'Hi! Here are your suggestions,\n '
    for i in range(0, len(res_data)):
        ans += "{}. {}, located at {}\n".format(i + 1, res_data[i]['name'], res_data[i]['address'])
    return ans


def lambda_handler(event=None, context=None):
    queue = sqs.get_queue_by_name(QueueName='restaurant_chatbot')
    messages = queue.receive_messages(MessageAttributeNames=['All'])
    for msg in messages:
        print("Received message: %s: %s", msg.message_id, msg.body)
    # response = sqs.get_queue_url(QueueName='Q1')
    # queue_url = response['QueueUrl']
    # print(queue_url)
    # message = None
    # Receive a message from SQS queue
    # response = sqs.receive_message(
    #     QueueUrl=queue_url,
    #     AttributeNames=[
    #         'SentTimestamp'
    #     ],
    #     MaxNumberOfMessages=1,
    #     MessageAttributeNames=[
    #         'All'
    #     ],
    #     VisibilityTimeout=0,
    #     WaitTimeSeconds=0
    # )
    try:
        message = messages[0]
        # receipt_handle = message['ReceiptHandle']
        # Delete received message from queue
        # sqs.delete_message(
        #     QueueUrl=queue_url,
        #     ReceiptHandle=receipt_handle
        # )
        location = message.message_attributes.get('Location').get('StringValue')
        cuisine = message.message_attributes.get('Cuisine').get('StringValue')
        dining_date = message.message_attributes.get('DiningDate').get('StringValue')
        dining_time = message.message_attributes.get('DiningTime').get('StringValue')
        num_people = message.message_attributes.get('PeopleNum').get('StringValue')
        phone = message.message_attributes.get('Phone').get('StringValue')
        print(location, cuisine, dining_date, dining_time, num_people, phone)
        ids = search(cuisine)
        ids = list(map(lambda x: x['_id'], ids))
        rest_details = get_restaurant_data(ids)
        send_sms("+1"+phone, rest_details)
        message.delete()
    except:
        print("SQS queue is now empty")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda LF2!')
    }

lambda_handler()