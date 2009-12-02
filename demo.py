#!/usr/bin/env python2.5

import sys
import time
from random import random
import asyncore
import pika

N = 15
Q = "forw_test_1"

def usage():
    print 'Usage: %s --step1|--step2' % sys.argv[0]

if len(sys.argv) != 2:
    usage()
    sys.exit(1)

creds = pika.PlainCredentials('guest', 'guest')
jerry_conn = pika.AsyncoreConnection(pika.ConnectionParameters(
                'localhost', port=65001, credentials=creds))
tom_conn = pika.AsyncoreConnection(pika.ConnectionParameters(
                'localhost', port=65002, credentials=creds))


# will publish to jerry, expect to consume from tom
publisher_ch = jerry_conn.channel()
consumer_ch = tom_conn.channel()

# create queues on both brokers first
if sys.argv[-1] == '--step1':
    for ch in (publisher_ch, consumer_ch):
        print ch.queue_declare(queue=Q, durable=True,
                            exclusive=False, auto_delete=False)
    print 'ok. step 1 done. run remsh code now. then re-run this with --step2'
    sys.exit(0)

if sys.argv[-1] != '--step2':
    usage()
    sys.exit(1)

sent = { }

def handle_delivery(ch, meth, header, body):
    global sent
    print time.asctime(), 'received %s' % body
    print sent
    try: del(sent[body])
    except KeyError: pass
    ch.basic_ack(delivery_tag=meth.delivery_tag)

for i in range(N):
    body = str(random())
    publisher_ch.basic_publish(exchange='', routing_key=Q, body=body)
    sent[body] = 1

attempts = N
while attempts > 0:
    attempts -= 1
    print attempts
    asyncore.poll2(timeout=1.0)
    if len(sent) == N and len(consumer_ch.callbacks) == 0:
        print 'Starting consumer'
        consumer_ch.basic_consume(handle_delivery, queue=Q)
    elif len(sent) == 0:
        print 'All consumed!'
        break
    
if attempts == 0:
    print 'failed. did you run step1? did you run remsh stuff?'
else:
    print 'ok'

# exiting
jerry_conn.close()
tom_conn.close()
asyncore.loop()


