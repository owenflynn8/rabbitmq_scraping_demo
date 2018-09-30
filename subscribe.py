# This program uses the Beautiful soup library to scrape the imdb top 250 moives of all time and let the user search by director
#!/usr/bin/env python
import pika
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import sqlite3
import sys

conn = sqlite3.connect('imdb.db') # This line creates the sqlite database
c = conn.cursor()

def create_table(): 
    c.execute('CREATE TABLE IF NOT EXISTS list(director TEXT, movie TEXT)')

def data_entry(director,movie):
    c.execute("INSERT INTO list (director, movie) VALUES(?, ?)", (director,movie))
    conn.commit()
    
key = sys.argv[1] + ' ' + sys.argv[2]
director = key + '*'

def scrape_function(director):
    create_table()
    
    url = "http://www.imdb.com/chart/top"
    content = urlopen(url).read() # source code of the web page you want to scrape
    soup = BeautifulSoup(content,"html.parser") # definition of beautiful soup parser 
    container = soup.find_all('a', {'title': re.compile(director)}) 
   
    # this section of the code just grabs just the names of the movies
    for i in range(0,len(container)):
        newDirector = director.replace("*","")
        movie = container[i].text 
        data_entry(newDirector,movie)

    return;

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_logs',
                         type='direct')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

if not key:
    sys.stderr.write("Usage: %s [info] [warning] [error]\n" % sys.argv[0])
    sys.exit(1)

channel.queue_bind(exchange='direct_logs', queue=queue_name, routing_key=key)

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    scrape_function(director)
    print("added " + key + " to database")

channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()
