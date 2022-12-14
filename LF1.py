import boto3
import datetime
import dateutil.parser
import json
import math

def lambda_handler(event, context):
    return search_intent(event)


def search_intent(event_info):
    intent_name = event_info['currentIntent']['name']
    if intent_name == 'GreetingIntent':
        return greeting_intent(event_info)
    elif intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions_intent(event_info)
    elif intent_name == 'ThankYouIntent':
        return thank_you_intent(event_info)

    raise Exception('Intent with name ' + intent_name + ' not supported')


def greeting_intent(event_info):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Hi there, how can I help you?'}
        }
    }

  
def thank_you_intent(event_info):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'You are welcome!'}
        }
    }


def dining_suggestions_intent(event_info):
    location = get_slots(event_info)["Location"]
    cuisine = get_slots(event_info)["Cuisine"]
    count_people = get_slots(event_info)["People"]
    date = get_slots(event_info)["DiningDate"]
    time = get_slots(event_info)["DiningTime"]
    PhoneNumber = get_slots(event_info)["PhoneNumber"]
    Email=get_slots(event_info)["Email"]
    
    source = event_info['invocationSource']

    if source == 'DialogCodeHook':
        slots = get_slots(event_info)

        validation_result = validate_slots(location, cuisine,count_people,  date, time, PhoneNumber,Email)
        

        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(event_info['sessionAttributes'],
                              event_info['currentIntent']['name'],
                              slots,
                              validation_result['violatedSlot'],
                              validation_result['message'])

        if event_info[
            'sessionAttributes'] is not None:
            output_session_attributes = event_info['sessionAttributes']
        else:
            output_session_attributes = {}

        return delegate_return(output_session_attributes, get_slots(event_info))

    sqs = boto3.client('sqs')
    sqs_url = 'https://sqs.us-east-1.amazonaws.com/751084578644/Q1'
    slots = event_info["currentIntent"]['slots']
    attributes = {
        'Cuisine': {
            'DataType':'String',
            'StringValue': slots['Cuisine']
        },
        'PhoneNumber': {
            'DataType': 'Number',
            'StringValue': slots['PhoneNumber']
        },
        'DiningDate': {
            'DataType': 'String',
            'StringValue': slots['DiningDate']
        },
        'People': {
            'DataType': 'Number',
            'StringValue': slots['People']
        },
        
        'DiningTime': {
            'DataType': 'String',
            'StringValue': slots['DiningTime']
        },
        'Location': {
            'DataType': 'String',
            'StringValue': slots['Location']
        },
        'Email': {
            'DataType': 'String',
            'StringValue': slots['Email']
        },
    }
    sqs.send_message(QueueUrl=sqs_url, MessageBody="message from LF1", MessageAttributes=attributes)

    return close(event_info['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'You???re all set. Expect my suggestions shortly! Have a good day.'})
    # return {
    #     'dialogAction': {
    #         "type": "ElicitIntent",
    #         'message': {
    #             'contentType': 'PlainText',
    #             'content': 'You are ordering7!'}
    #     }
    # }


def get_slots(event_info):
    return event_info['currentIntent']['slots']


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def validation_res(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def delegate_return(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


def validate_slots(location, cuisine,count_people,  date, time,phonenumber,email):
    cuisines = ['italian', 'chinese', 'mexican', 'american','korean', 'thai', 'arab', 'Caribbean','vietnamese', 'indian']
    if cuisine is not None and cuisine.lower() not in cuisines:
        return validation_res(False,'Cuisine','Cuisine not found. Please try another.')

    if count_people is not None:
        count_people = int(count_people)
        if count_people > 20 or count_people < 0:
            return validation_res(False,'People','Maximum 20 people allowed. Please try again')

    if date is not None:
        if datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return validation_res(False, 'DiningDate', 'Sorry wrong date inserted, Please enter date again (Future dates only)')

    if time is not None:
        if len(time) != 5:
            return validation_res(False, 'DiningTime', None)

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)

        if hour < 7 or hour > 21:
            return validation_res(False, 'DiningTime','We accept time from 7 am to 9 pm. Please specify a time during this range?')
    
    if phonenumber is not None:
        if len(phonenumber) != 10:
            return validation_res(False, 'PhoneNumber','Please specify valid mobile number with 10 digits')
            
    if location is not None:
        if location.lower() !="manhattan":
            return validation_res(False,'Location','Only available in Manhattan, please try again')
            
    # if email is None:
    #     return validation_res(False,'Email','Please enter an email')

    return validation_res(True, None, None)