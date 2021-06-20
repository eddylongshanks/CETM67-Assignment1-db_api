""" Script for db API
"""

from flask import Flask, request, jsonify
from flask_restful import Api, Resource, reqparse, abort
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

            f = open('db_log.txt', 'w')
            f.write(str(data))
            f.close()

            table = dynamodb.Table('Enquiry')
            table.put_item(
                Item = data
            )

            return 'Enquiry added'

        except Exception as e:
            f = open('db_log.txt', 'w')
            f.write(str(e))
            f.close()
            raise


class AddEnquirySNS(Resource):
    def post(self):
        try:
            data = json.loads(request.data)

            header = request.headers.get('X-Amz-Sns-Message-Type')
            # Perform check for subscription confirmation request, subscribe to the SNS topic
            if header == 'SubscriptionConfirmation' and 'SubscribeURL' in data:
                r = requests.get(data['SubscribeURL'])

            #if header == 'Notification':
            enquiry = process_sns(data)

            table = dynamodb.Table('Enquiry')
            table.put_item(
                Item = enquiry
            )
            
            return enquiry

        except Exception as e:
            f = open('db_log.txt', 'w')
            f.write(str(e))
            f.close()
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
    js = json.loads(msg['Message'])

    # Generate a Guid for the ID
    js['id'] = str(uuid.uuid4())

    return js

if __name__ == "__main__":
    app.run(debug=True)