import json
import boto3
import random
import requests
#WORKING CODE
import base64
import urllib
from urllib import request, parse
# import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
# from requests_aws4auth import AWS4Auth
import warnings
from elasticsearch.exceptions import ElasticsearchWarning
import logging
# from botocore.vendored import requests
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
message = ''

TWILIO_SMS_URL = "https://api.twilio.com/2010-04-01/Accounts/{}/Messages.json"
TWILIO_ACCOUNT_SID = "**"
TWILIO_AUTH_TOKEN = "**"


host = "search-dines-h5wgwg5jyqiqi64nly5gsobl5q.us-east-1.es.amazonaws.com"
endpoint = 'https://search-dines-h5wgwg5jyqiqi64nly5gsobl5q.us-east-1.es.amazonaws.com'
# credentials = boto3.Session().get_credentials()
# aws_session_token = credentials.token
headers = { "Content-Type": "application/json" }
# awsauth=AWS4Auth(credentials.access_key,credentials.secret_key, 'us-east-1', 'es', session_token = aws_session_token)

# index = "elastic-search"

# url = endpoint + '/' + index + '/_search'


def lambda_handler(event, context):
    
    
    
    warnings.simplefilter('ignore', ElasticsearchWarning)

    sqs = boto3.client('sqs')
    dynamodb = boto3.resource('dynamodb')
    queue_url = "https://sqs.us-east-1.amazonaws.com/751084578644/Q1"

    # polling messaging from sqs
    response = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=0,
    WaitTimeSeconds=0)
    
    print("This is the response from sqs ->",response)
    message = response['Messages'][0]
    cuisine = message['MessageAttributes'].get('Cuisine').get('StringValue')
    print(cuisine)
    #extracting other things from sqs
    phoneNumber = message['MessageAttributes'].get('PhoneNumber').get('StringValue')
   # email = message['email']['StringValue']
    location=message['MessageAttributes'].get('Location').get('StringValue')
    numOfPeople=message['MessageAttributes'].get('People').get('StringValue')
    date=message['MessageAttributes'].get('DiningDate').get('StringValue')
    time=message['MessageAttributes'].get('DiningTime').get('StringValue')
    phoneNumber = "+1" + phoneNumber
    if not cuisine or not phoneNumber:
        logger.debug("No Cuisine or PhoneNum key found in message")
        return
    
    
    sqs2 = boto3.client('sqs')
    queue_url2 = "https://sqs.us-east-1.amazonaws.com/751084578644/Q1"
    
    reciept_handle = response['Messages'][0]['ReceiptHandle']
    sqs2.delete_message(QueueUrl = queue_url2, ReceiptHandle = reciept_handle )
    
    
     # The OpenSearch domain endpoint with https://
    index = 'restaurants'
    url = endpoint + '/' + index + '/_search'
    print(url)
    query = {
        "query": {
        "match": {
        "Cuisine": cuisine 
        }
    }
    }
    
    
    
    # Elasticsearch 6.x requires an explicit Content-Type header
    headers = { "Content-Type": "application/json" }
    # Make the signed HTTP request
    r = requests.get(url, auth=('**','**'), headers=headers, data=json.dumps(query))
    data = json.loads(r.content.decode('utf-8'))
    
    print("datadatadatadata",data)
    esData=[]
    try:
        esData = data["hits"]["hits"]
    except KeyError:
        logger.debug("Error extracting hits from ES response")
    print(esData)
    # extract bID from AWS ES
    
    ids = []
    for restaurant in esData:
        ids.append(restaurant["_source"]["RestaurantID"])
    print(ids)
    messageToSend = 'Hello! Here are my {cuisine} restaurant suggestions in {location} for {numPeople} people, for {diningDate} at {diningTime}: '.format(
            cuisine=cuisine,
            location=location,
            numPeople=numOfPeople,
            diningDate=date,
            diningTime=time,
        )
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    itr = 1
    prevRestaurants=""
    Restaurants=[]
    for id in ids:
        if itr == 6:
            break
        response = table.get_item(Key={'business_id': id})
        print(response)
        if response is None or "Item" not in response.keys():
            continue
        print(response)
        item = response['Item']
        restaurantMsg = '' + str(itr) + '. '
        name = item["name"]
        print(name)
        prevRestaurants+=str(name)+","
        address = item["address"]
        print(address)
        
        restaurantMsg += name +', located at ' + address +'. '
        messageToSend += restaurantMsg
        print(messageToSend)
        
        itr += 1
        
    body = messageToSend
    
    Sample_Number = '**'
    to_number = phoneNumber
    from_number = '**'
    
    
    populated_url = TWILIO_SMS_URL.format(TWILIO_ACCOUNT_SID)
    post_params = {"To": to_number, "From": from_number, "Body": body}
    
    data = parse.urlencode(post_params).encode()
    req = request.Request(populated_url)
    
    authentication = "{}:{}".format(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    base64string = base64.b64encode(authentication.encode('utf-8'))
    req.add_header("Authorization", "Basic %s" % base64string.decode('ascii'))
    
    try:
        # perform HTTP POST request
        with request.urlopen(req, data) as f:
            print("Twilio returned {}".format(str(f.read().decode('utf-8'))))
    except Exception as e:
        # something went wrong!
        return e
    
    
    if prevRestaurants:
        response = table.put_item(
                Item={
                                'restaurant_id':"prevRestaurants" ,
                                'name':prevRestaurants[:-1],
                })
    messageToSend += "Enjoy your meal!!"
    print(messageToSend)
    print(phoneNumber)
