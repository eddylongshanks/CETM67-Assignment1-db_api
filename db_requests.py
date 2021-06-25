""" DB Requests
"""

import json
from requests import post, get

BASE_URL = "http://127.0.0.1:5000"
#BASE_URL = "http://ec2-18-130-251-69.eu-west-2.compute.amazonaws.com"

# GET all enquiries
# response = get(BASE_URL + "/get-all-enquiries")
# response_data = json.dumps(response.text)
# print(response.text)


# CREATE a new enquiry
data = {
    'first_name': 'Bob',
    'last_name': 'Smith',
    'building': '11',
    'street': 'Indigo Avenue',
    'town': 'Newcastle',
    'county': 'Tyne and Wear',
    'postcode': 'NE2 3JM',
    'telephone_number': '07777777778',
    'email_address': 'me@me.com',
    'preferred_time_to_contact': 'S',
    'annual_income': '30000',
    'loan_amount': '50000',
    'property_value': '1000000',
    'mortgage_type': 'RM',
    'ltv_value': '50'
}

data1 = {
    "first_name": "vs_DB_API1",
    "last_name": "Smith",
    "building": "11",
    "street": "Indigo Avenue",
    "town": "Newcastle",
    "county": "Tyne and Wear",
    "postcode": "NE2 3JM",
    "telephone_number": "07777777778",
    "email_address": "me@me.com",
    "preferred_time_to_contact": "S",
    "annual_income": "30000",
    "loan_amount": "50000",
    "property_value": "1000000",
    "mortgage_type": "RM",
    "ltv_value": "50"
}

# Simulate sending from SNS
data2 = {
    "Message": "{\"first_name\": \"pc3\",\"last_name\": \"Stevenson\",\"ltv_value\": \"34.89\",\"telephone_number\": \"07777456278\"}"
}

sns_header = {"X-Amz-Sns-Message-Type": "Notification"}

data3 = {
    "first_name": "pc1",
    "last_name": "Stevenson",
    "telephone_number": "07777456278"
}

data4 = "{'Message':{'first_name':'pc2','last_name':'Stevenson','telephone_number':'07777456278'}\}"

data5 = "{'Type': 'Notification', 'MessageId': 'a2487ca9-709e-52d2-a489-25ed7879d431', 'TopicArn': 'arn:aws:sns:us-east-1:118394980800:cetm67-odip-sub-v1-dev', 'Message': '\"{\\'first_name\\': \\'sns4\\',\\'last_name\\':\\'Stevenson\\',\\'telephone_number\\':\\'07777456278\\'}\"', 'Timestamp': '2021-06-16T14:11:51.727Z', 'SignatureVersion': '1', 'Signature': 'MQkmhp7c7f6HeGSeG3LSfqRLaN7Mwo8WgMlDfti7voeOYZKkLxADzYuN5jXIMr5TU0kpr5fBYB41dFofZeh06C5z3DxDlYs0MAejHdZRFAHbuwnL/iXR8rwbR3J/77er1fCzanVjhQ4EKGcPIZWiXqhQtAAhPXip4xWEt15cC5PpAToRbcHo70awUslnp2xrXGA7kS0oydKfHFQh1vZIc9X5AatkKQXU2zyWxsEwZH64KNzvIosZQVvL8BSHoIj0544GWTuqXtFTev8PiKCf+4EsOM1x3r6+AGA6J+5WsABN+Xe9fBN8NhnPQSQcOj1GX4AgN0DUjeioJoWTGtOEWw==', 'SigningCertURL': 'https://sns.us-east-1.amazonaws.com/SimpleNotificationService-010a507c1833636cd94bdb98bd93083a.pem', 'UnsubscribeURL': 'https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:118394980800:cetm67-odip-sub-v1-dev:9330baa0-4c35-4b65-9419-e0cb0a34ec16'}"

#response = get(BASE_URL + "/get-all-enquiries")

#response = post(BASE_URL + "/add-enquiry", headers=sns_header, json = data1)
response = post(BASE_URL + "/add-enquiry", json = data1)
print(response.text)
