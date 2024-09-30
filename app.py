import json

from botocore.exceptions import ClientError
from chalice import Chalice
from chalicelib import *
import boto3
import time
import redis

app = Chalice(app_name='webhook-trade-bot')

sqs = boto3.client('sqs', region_name='eu-west-2')
if REDIS_ENABLED:
    db = redis.Redis(host=REDIS_HOST, port= REDIS_PORT, decode_responses=True)


# {'_action': 'OPEN',
#                   '_type': 0,
#                   '_symbol': 'EURUSD',
#                   '_price': 0.0,
#                   '_SL': 500, # SL/TP in POINTS, not pips.
#                   '_TP': 500,
#                   '_comment': self._ClientID,
#                   '_lots': 0.01,
#                   '_magic': 123456,
#                   '_ticket': 0}
@app.route('/webhook', methods=['POST'])
def receive():
    request = app.current_request
    webhook_message = request.json_body

    message_deduplication_id = webhook_message['symbol'] + str(webhook_message['price']) + str(webhook_message['time'])

    if webhook_message['TVkey'] != TV_KEY:
        return {}

    response = send_message(message_body=convert_mql(webhook_message), message_deduplication_id=message_deduplication_id)

    return {
        'message': "Executed Successfully",
        'webhook_message': webhook_message,
        'response': response
    }

# web_msg = {
#      "TVkey": "123",
#      "symbol": "EURUSD",
#      "operation": "buy",
#      "action": "open",
#      "price" : 1234,
#      "volume": 0.01,
#      "time": "1234",
#      "timenow": "1234"
#     }
# return convert_mql(web_msg)
@app.route('/receive', methods=['POST'])
def send():
    msg = receive_messages()
    # current_date_time = datetime.now()
    # date_time = current_date_time.strftime("%d/%m/%Y %H:%M:%S")
    current_time = time.time() * 1000
    if msg is not None:
        while msg is not None and current_time - float(msg['Attributes']['SentTimestamp']) > 10000:
            msg = receive_messages()
        return return_msgs(msg['Body']) if 'Body' in msg else {}
    else:
        return {}

@app.route('/ticket', methods=['POST'])
def save_ticket():
    ticket = get_json_body()
    set_ticket(ticket)

@app.route('/ticket', methods=['DELETE'])
def delete_ticket():
    ticket = get_json_body()
    remove_ticket(ticket)


def send_message(message_body, message_attributes=None, message_deduplication_id=None):
    if not message_attributes:
        message_attributes = {}

    try:
        response = sqs.send_message(QueueUrl=QUEUE_URL,
                                    MessageBody=message_body, MessageAttributes=message_attributes,
                                    MessageDeduplicationId=message_deduplication_id,
                                    MessageGroupId=MessageGroupId
                                    )
    except ClientError as error:
        raise error
    else:
        return response


# {
#  "TVkey": "SomeKeyToAuthenticate",
#  "symbol": "{{ticker}}""EURUSD"",
#  "operation": 'buy',
#  "action": "open",
#  'price' : 1234
#  "volume": {{volume}},
#  "time": "{{time}}",
#  "timenow": "{{timenow}}"
#  }
# return ({
#     '_action_type': 'TRADE',
#     '_action': action,
#     '_type': op_type,
#     '_symbol': webhook_message['symbol'],
#     '_price': webhook_message['price'],
#     '_SL': 0,  # SL/TP in POINTS, not pips.
#     '_TP': 0,
#     '_comment': 'auto trade',
#     '_lots': 0.01 if webhook_message['volume'] is None else webhook_message['volume'],
#     '_magic': 12345,
#     '_ticket': 0})
def convert_mql(webhook_message, ticket="0"):
    if webhook_message['operation'].lower() == 'buy':
        op_type = 'OP_BUYLIMIT'
    elif webhook_message['operation'].lower() == 'sell':
        op_type = 'OP_SELLLIMIT'
    elif webhook_message['operation'].lower() == 'buy_m':
        op_type = 'OP_BUY'
    else:
        op_type = 'OP_SELL'

    if webhook_message['action'].lower() == 'open':
        action = 'OPEN'
    elif webhook_message['action'].lower() == 'close':
        action = 'CLOSE_PARTIAL'
    elif webhook_message['action'].lower() == 'close_m':
        action = 'CLOSE'
    else:
        action = 'MODIFY'
    # _action_type|_action|_type|_symbol|_price|_SL|_TP|_comment|_lots|_magic|_ticket
    li = ('TRADE',action,op_type,webhook_message['symbol'],
          str(webhook_message['price']),'0','0','auto trade',
          str(0.01) if 'volume' not in webhook_message else str(webhook_message['volume']),
          '12345',ticket)
    return '|'.join(li)



def receive_messages():
    try:
        res = sqs.receive_message(QueueUrl=QUEUE_URL,
                                        AttributeNames=["MessageDeduplicationId","MessageGroupId","SentTimestamp"],
                                        MessageAttributeNames=["All"],
                                        MaxNumberOfMessages=1)
        if 'Messages' in res:
            for msg in res['Messages'] :
                if msg['Attributes']['MessageGroupId'] == MessageGroupId:
                    # print(f"Received message: {msg}")
                    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg['ReceiptHandle'])
                    return msg
    except ClientError as error:
        print(f"Couldn't receive messages because of error: {error}")
        raise error


def delete_message(msg):
    try:
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg['ReceiptHandle'])
    except ClientError as error:
        print(f"Couldn't delete messages because of error: {error}")

def set_ticket(ticket):
    if REDIS_ENABLED and '_symbol' in ticket and '_ticket' in ticket:
        db.lpush( ticket['_symbol'],ticket['_ticket'] )
        db.set(ticket['_ticket'], ticket['_symbol'])

def get_tickets(symbol):
    if REDIS_ENABLED and symbol is not None:
        return db.lrange(symbol, 0, -1)


def remove_ticket(ticket):
    if REDIS_ENABLED and '_ticket' in ticket:
        sym = db.get(ticket['_ticket'])
        if sym is not None:
            db.lrem(str(sym), 0, ticket['_ticket'])
            db.delete(ticket['_ticket'])

# _action_type|_action|_type|_symbol|_price|_SL|_TP|_comment|_lots|_magic|_ticket
def return_msgs(msg):
    msql = msg.split("|")
    action = msql[1].lower()
    if action == "close" or action == "close_partial":
        return set_ticket_value(msql, get_tickets(msql[3]))
    else:
        return "|".join(msql)

def set_ticket_value(msql, tickets):
    for ticket in tickets:
        msql[-1] = ticket
        return "|".join(msql)

def get_json_body():
    request = app.current_request
    body = request.raw_body
    if isinstance(body, bytes):
        body = body.decode('ascii')
    body = body.replace("\x00","")
    return json.loads(body)
