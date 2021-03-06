import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('lex-runtime')
    msg = event['messages'][0]['unstructured']['text']
    user_id = 'root'
    lex_bot = 'Diningconcierge'
    bot_alias = 'restaurantfinder'

    res = client.post_text(
        botName = lex_bot,
        botAlias = bot_alias,
        userId = user_id,
        inputText = msg
    )
    response = {
        "messages" : [{
            "type" : "unstructured",
            "unstructured" : {
                "text" : res['message']
            }
        }]
    }
    return response
