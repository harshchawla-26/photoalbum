import json
import boto3
import logging

def lambda_handler(event, context):
    print(event)
    print("Hi")
    lex = boto3.client('lex-runtime')
    
    lex_resp = lex.post_text(
        botName = 'dinbot',
        botAlias = 'dinbotaliass',
        userId = 'user01',
        inputText = event['messages'][0]['unstructured']['text'],
        activeContexts=[]
        )
    response = {
        "messages":
            [
                {"type": "unstructured",
                "unstructured":
                    {
                        "text": lex_resp['message']
                    }
                }
            ]
    }
    return response