from confluent_kafka import Consumer
import sys
import socket
import json
import copy
from confluent_kafka import Producer


from confluent_kafka import Consumer

confc = {'bootstrap.servers': "localhost:9092",
        'group.id': "testconsumer",
        'auto.offset.reset': 'earliest',
        'enable.auto.commit':'true'}

consumer = Consumer(confc)



running = True

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


def decode(msg):
    return msg.value().decode('utf-8')

def get_json(msg):
    return json.loads(decode(msg))

def clean_jumpline(mstring):
    return mstring.replace('\n','')


def msg_process(msg):
    print(get_json(msg))

    #print(json_msg)


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False


basic_consume_loop(consumer,['test_confluent1'])