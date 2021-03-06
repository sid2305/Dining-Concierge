import yelpapi
from yelpapi import YelpAPI
import json
import boto3
from decimal import Decimal
import requests
import datetime

dynamodb = boto3.resource('dynamodb',region_name='us-west-2')
table = dynamodb.Table("yelp-restaurants")

region = 'us-west-2'
headers = {"Content-Type": "application/json"}

# write private api_key to access yelp here
api_key = 'kNnGpcOEG5EP7MUV5-1oO8kjKxlRdjYoM8DQyAD2xj1yRe7VCqWpDW986Cf-qIo7xM20ceqbyYSfUB05os8ZYikXHL_1BzrOeEYQoHaI-lr2gNgfAINtDmSkJTQ_YHYx'

yelp_api = YelpAPI(api_key)

data = ['id', 'name', 'review_count', 'rating', 'coordinates', 'address1', 'zip_code', 'display_phone']
es_data = ['id']

# cuisines = ["thai", "chinese", "mexican"]
cuisines = ["italian"]
# cuisines = ["american"]
# cuisines = ["mexican"]


def fill_database(response, cuisine):
    new_response = json.loads(json.dumps(response), parse_float=Decimal)
    counter = 0
    for business in new_response["businesses"]:
        keyCheck  = table.get_item(Key={'restaurantID':business['id']})
        if 'Item' in keyCheck:
            continue
        try:
            details = {
                "insertedAtTimestamp": str(datetime.datetime.now()),
                "restaurantID": business["id"],
                "alias": business["alias"],
                "name": business["name"],
                "rating": Decimal(business['rating']),
                "numReviews": int(business["review_count"]),
                "address": business["location"]["display_address"],
                "latitude": str(business["coordinates"]["latitude"]),
                "longitude": str(business["coordinates"]["longitude"]),
                "zip_code": business['location']['zip_code'],
                "cuisine": cuisine,
                "city:": business['location']['city']
            }

            table.put_item(Item=details)
            counter = counter + 1

        except Exception as e:
            print("Error", e)
            exit(1)
    print(counter)

def get_data(event=None, context=None):
    for cuisine in cuisines:
        for x in range(0, 1000, 50):
            response = yelp_api.search_query(term=cuisine, location='manhattan', limit=50, offset=x)
            # print(response)
            fill_database(response, cuisine)

get_data()