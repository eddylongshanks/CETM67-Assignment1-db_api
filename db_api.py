""" Script for db API
"""

from flask import Flask, request, jsonify
from flask_restful import Api, Resource, reqparse, abort
from decimal import Decimal
import json
import requests
import boto3
import botocore.exceptions
import uuid

dynamodb = boto3.resource('dynamodb')

app = Flask(__name__)
api = Api(app)

## Resources ##
class GetAllEnquiries(Resource):
    def get(self):
        # Get table
        try:
            table = dynamodb.Table('Enquiry')
            enquiries = table.scan()
            return enquiries['Items']
        except:
            raise


class AddEnquiry(Resource):
    def post(self):
        try:
            data = json.loads(request.data)
            data['id'] = get_guid()
            log(data)

            table = dynamodb.Table('Enquiry')
            table.put_item(
                Item = data
            )

            return 'Enquiry added'

        except Exception as e:
            log(e)
            raise


class AddEnquirySNS(Resource):
    def post(self):
        try:
            data = json.loads(request.data)

            header = request.headers.get('X-Amz-Sns-Message-Type')
            # Perform check for subscription confirmation request, subscribe to the SNS topic
            if header == 'SubscriptionConfirmation' and 'SubscribeURL' in data:
                r = requests.get(data['SubscribeURL'])

            if header == 'Notification':
                enquiry = process_sns(data)

            log(enquiry)

            table = dynamodb.Table('Enquiry')
            table.put_item(
                Item = enquiry
            )
            
            return enquiry

        except Exception as e:
            log(e)
            raise


class GetLog(Resource):
    def get(self):
        f = open('db_log.txt', 'r')
        return f.read()


class HealthCheck(Resource):
    def get(self):
        return 'DB API Available'

## Routing ##
api.add_resource(HealthCheck, '/')
api.add_resource(GetLog, '/log')
api.add_resource(GetAllEnquiries, '/get-all-enquiries')
api.add_resource(AddEnquiry, '/add-enquiry')
api.add_resource(AddEnquirySNS, '/add-enquiry-sns')

# Methods

def process_sns(msg):
    # Converts the contents of the message string to a dictionary object
    js = json.loads(msg['Message'], parse_float=Decimal)
    js['id'] = get_guid()

    return js

def get_guid():
    # Generate a Guid for the ID
    return str(uuid.uuid4())

def log(data_to_save):
    # Logs data to a local file for debugging
    f = open('db_log.txt', 'w')
    f.write(str(data_to_save))
    f.close()

if __name__ == "__main__":
    app.run(debug=True)