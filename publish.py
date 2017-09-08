#!/usr/bin/env python
import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_logs',
                         type='direct')

key = sys.argv[1] + ' ' + sys.argv[2]

message = 'to scraper'
channel.basic_publish(exchange='direct_logs',
                      routing_key=key,
                      body=message)
print(" [x] Sent %r:%r" % (key, message))
connection.close()
