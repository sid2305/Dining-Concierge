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
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 'us-west-2', 'es',session_token=credentials.token)
# print(credentials.access_key,credentials.secret_key)
sqs = boto3.resource('sqs',region_name='us-west-2')
es = Elasticsearch(SEARCH_URL, http_auth=awsauth, connection_class=RequestsHttpConnection)
# check = es.get(index="restaurants", doc_type="Restaurant", id='-KahGyU9G7JT0JmoC_Yc0Q')

def sendsms(number, message):
    # sns = boto3.client(
    #     'sns',
    #     aws_access_key_id=os.getenv('AWS_SERVER_PUBLIC_KEY'),
    #     aws_secret_access_key=os.getenv('AWS_SERVER_SECRET_KEY')
    # )
    send_sms = boto3.client('sns',region_name='us-west-2')
    # send_sms = boto3.resource('sns', region_name='us-west-2')
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
    # requestBody = {}
    # requestBody['size'] = SAMPLE_N
    # requestBody['query'] = {}
    # requestBody['query']['bool'] = {}
    # requestBody['query']['bool']['must'] = list([{
    #     'match': {
    #         'cuisine': cuisine
    #     }
    # # }])
    # data = es.search(index="restaurants", body=requestBody)
    data = es.search(index="restaurants", body={"query": {"match": {'cuisine':cuisine}}})
    print("search complete", data['hits']['hits'])
    return data['hits']['hits']


def get_restaurant_data(ids):
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table('yelp-restaurants')
    # all_restaurants = []
    ans = 'Hi! Here are your suggestions,\n '
    i = 1
    for id in ids:
        if i<6:
            response = table.get_item(
                Key={
                    'restaurantID': id
                }
            )
            response_item = response.get("Item")
            restaurant_name = response_item['name']
            # restaurant_category = response_item['category']
            restaurant_address = response_item['address']
            restaurant_city = response_item['city:']
            restaurant_zipcode = response_item['zip_code']
            restaurant_rating = str(response_item['rating'])
            # restaurant_url = str(response_item['url'])
            # restaurant_phone = response_item['phone']
            # formatted_restaurant_info = restaurant_name + " " + restaurant_phone + '\n' + "Rating: " + restaurant_rating + ", " + restaurant_address + ", " + restaurant_city + ", " + restaurant_zipcode + " " + restaurant_url
            # all_restaurants.append(formatted_restaurant_info)
            ans += "{}. {}, located at {}\n".format(i, restaurant_name, restaurant_address)
            # return ans
            i += 1
        else:
            break
    print("db pass")
    return ans # string type


def lambda_handler(event=None, context=None):
    queue = sqs.get_queue_by_name(QueueName='restaurant_chatbot')
    messages = queue.receive_messages(MessageAttributeNames=['All'])
    # for msg in messages:
    #     print("Received message: %s: %s", msg.message_id, msg.body)
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
        sendsms("+1"+phone, rest_details)
        message.delete()
    except Exception as e:
        print(e)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda LF2!')
    }

lambda_handler()