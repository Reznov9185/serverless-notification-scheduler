import boto3
import json
import requests
import psycopg2
from chalice import Chalice, Rate
from psycopg2.extras import RealDictCursor

app = Chalice(app_name='serverless-notification-scheduler')
app.debug = True

# Declarations of SQS boto3 instances
SQS_CLIENT = boto3.client('sqs')
SQS_RESOURCE = boto3.resource('sqs')

# Declaration of Dynamodb boto3 instances
DYNAMO_RESOURCE = boto3.resource('dynamodb')

@app.route('/')
def index():
    return {'status': 200}

def get_configs():
    dynamo_table = DYNAMO_RESOURCE.Table('test_db_creds')
    db_creds = dynamo_table.get_item(Key={'name': 'stage_creds'})
    credentials = ''
    if 'Item' in db_creds:
        credentials = db_creds['Item']
    return credentials

def get_expired_users_queries():
    dynamo_table = DYNAMO_RESOURCE.Table('lily_db_creds')
    db_creds = dynamo_table.get_item(Key={'name': 'query_for_expired_users'})
    credentials = ''
    if 'Item' in db_creds:
        credentials = db_creds['Item']
    return credentials

def connect_db():
    credentials = get_configs()
    db_conn = ''
    try:
        db_conn = psycopg2.connect(database=credentials['db_name'],
                                user=credentials['db_user'],
                                password=credentials['db_password'],
                                host=credentials['db_host'],
                                port=credentials['db_port'])
    except Exception as e:
        db_conn = str(e)
    return db_conn


# Trigger for SQS queue
@app.on_sqs_message(queue='test-queue', batch_size=1)
def handle_sqs_message(event):
    data = ''
    for record in event:
        app.log.debug("Received message with contents: ", record.body)
        message = json.loads(record.body)
        data = send_message_to_customer(fb_id=message['fb_id'], msg_text='Your Lily payment is expired!')
    app.log.debug("Processed SQS entry returned with: ", json.dumps(data))


def filtered_users_to_notify():
    data = ''
    try:
        expired_users_queries = get_expired_users_queries()
        cur = connect_db().cursor(cursor_factory=RealDictCursor)
        cur.execute(expired_users_queries['sql_query'])
        data = cur.fetchall()
    except Exception as e:
        data = str(e)
    return data


@app.route('/cleanup-queue/{queue_name}')
def clean_queue(queue_name):
    data = ''
    try:
        queue_url = SQS_CLIENT.get_queue_url(QueueName=queue_name)
        if 'QueueUrl' in queue_url:
            data = SQS_CLIENT.purge_queue(
                QueueUrl= queue_url['QueueUrl']
            )
    except Exception as e:
        data = str(e)
    return json.dumps(data)


@app.route('/users-with-expired-payments')
def fetch_payment_expiring_customers():
    notifiable_users = filtered_users_to_notify()
    return json.dumps(notifiable_users)


# TODO: Uncomment the next block, only when you want to enable the scheduler
# @app.schedule(Rate(1, unit=Rate.DAYS))
# def each_day(event):
#     app.log.debug("Event triggered: ", json.dumps(event.to_dict()))
#     add_users_to_sqs()


@app.route('/add-users-to-the-queue')
def add_users_to_sqs():
    queue = SQS_RESOURCE.get_queue_by_name(QueueName='test-queue')
    fetch_users_to_notify = filtered_users_to_notify()
    response = ''
    for customer in fetch_users_to_notify:
        if 'fb_id' in customer:
            response = queue.send_message(MessageBody=json.dumps({'fb_id': customer['fb_id']}))

    return json.dumps(response)


@app.route('/fetch-msg-from-the-queue')
def fetch_msg_from_the_queue():
    data = ''
    try:
        queue_url = SQS_CLIENT.get_queue_url(QueueName='test-queue')
        if 'QueueUrl' in queue_url:
            attributes = SQS_CLIENT.get_queue_attributes(
                QueueUrl=queue_url['QueueUrl'],
                AttributeNames=['ApproximateNumberOfMessages']
            )
            approx_messages_in_queue = int(attributes['Attributes']['ApproximateNumberOfMessages'])
            if approx_messages_in_queue > 0:
                messages = SQS_CLIENT.receive_message(QueueUrl=queue_url['QueueUrl'])
                message = json.loads(messages['Messages'][0]['Body'])
                message = send_message_to_customer(fb_id=message['fb_id'], msg_text='message from lily sqs')
                # TODO: Need a check if the sending msg succeeds
                if 'Messages' in messages:
                    for msg in messages['Messages']:
                        if 'ReceiptHandle' in msg:
                            msg = SQS_RESOURCE.Message(queue_url['QueueUrl'], msg['ReceiptHandle'])
                            msg.delete()
            else:
                message = json.dumps({'msg': 'Nothing left on queue!'})
            data = message
    except Exception as e:
        data = str(e)
    return json.loads(data)


@app.route('/delete-fetched-messages-from-the-queue')
def delete_msg_from_sqs():
    queue_url = SQS_CLIENT.get_queue_url(QueueName='test-queue')
    count = 0
    if 'QueueUrl' in queue_url:
        messages = SQS_CLIENT.receive_message(QueueUrl=queue_url['QueueUrl'])
        if 'Messages' in messages:
            for msg in messages['Messages']:
                if 'ReceiptHandle' in msg:
                    msg = SQS_RESOURCE.Message(queue_url['QueueUrl'], msg['ReceiptHandle'])
                    msg.delete()
                    count += 1
    return {'deleted_msg_count': count}

@app.route('/send-message', methods=['POST'])
def send_message_to_customer(fb_id = None, msg_text = None):
    try:
        if (app.current_request is not None) and (app.current_request.json_body is not None):
            params = app.current_request.json_body
            client_id = params['fb_id'] if 'fb_id' in params else ''
            msg_text = params['message_text'] if 'message_text' in params else ''
            platform = params['platform'] if 'platform' in params else 'facebook'
        else:
            client_id = fb_id
            msg_text = msg_text
            platform = 'facebook'
        credentials = get_configs()
        fb_page_access_token = credentials['fb_page_access_token']
    except Exception as e:
        return str(e)
    if client_id and msg_text and fb_page_access_token and platform == 'facebook':
        url = 'https://graph.facebook.com/v2.6/me/messages?access_token=' + fb_page_access_token
        headers = {'content-type': 'application/json'}
        payload = {
            "messaging_type": "RESPONSE",
            "recipient": {
                "id": client_id
            },
            "message": {
                "text": msg_text
            }
        }
        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers)
            res = json.loads(response.text)
        except Exception as e:
            res = str(e)
        return json.dumps(res)
    else:
        return json.dumps({'error': 'Required Params(fb_id, message_text, fb_page_access_token) missing!'})

