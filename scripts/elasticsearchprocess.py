import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


def put_into_elasticsearch():
    host = "search-dineinconcierge-2q6wr7ab7bn5uoqzpbclerolhe.us-west-2.es.amazonaws.com"
    region = "us-west-2"
    service = "es"
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)

    es = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table("yelp-restaurants")
    response = None
    while True:
        if response is None:
            response = table.scan()
        else:
            # Scan from where you stopped previously.
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        counter = 0
        for business in response['Items']:
            if not response:
                # Scan from the start.
                response = table.scan()
            restaurantID = business["restaurantID"]
            doc = {
                "restaurantID": restaurantID,
                "cuisine": business["cuisine"]
            }
            es.index(
                index="restaurants",
                doc_type="Restaurant",
                id=restaurantID,
                body=doc,
            )
            check = es.get(index="restaurants", doc_type="Restaurant", id=restaurantID)
            if check["found"]:
                print("Index %s succeeded" % restaurantID)
            counter = counter + 1

put_into_elasticsearch()
