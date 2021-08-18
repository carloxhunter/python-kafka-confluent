import sys
import json
from confluent_kafka import Consumer
import psycopg2
from psycopg2 import Error
from decouple import config

DATABASE=config('db_database')
DB_USER=config('db_user')
DB_PASSWORD=config('db_password')
DB_HOST=config('db_host')
DB_PORT=config('db_port')
KAFKA_IP=config('kafka_ip')
KAFKA_PORT=config('kafka_port')


months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
def basic_consume_loop(stack, consumer, topics):
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
                msg_process(stack, msg, 'lc_person')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

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

def insert_postgress(data, cursor, connection):
    print(data[0])
    args_str = ','.join(['%s'] * len(data))
    sql = "INSERT INTO test_dk(id_cc, fecha, hora, acceso_id, nombre_comercial_acceso, piso, ins, outs ) VALUES {}".format(args_str)
    cursor.mogrify(sql, data).decode('utf8')
    cursor.execute(sql, data)
    connection.commit()
    print("Records inserted successfully into table")
    del data[:]

def msg_process(stack, msg, strs):
    json_msg = get_json(msg)
    json_msg['@timestamp']=clean_jumpline(json_msg['@timestamp'])
    lines_ids=json_msg[strs]['line_id'].split('|')
    counts=json_msg[strs]['count'].split('|')
    date_time=json_msg['@timestamp'].split(' ')
    month=str(months.index(date_time[1])+1)
    date = date_time[2]+'/'+month+'/'+date_time[4]
    time = date_time[3]
    #clave
    data = (json_msg[strs]['camera_id'], date, time, '-', '-', '-',
     counts[0], counts[1])
    try:
        stack.append(data)
        if len(stack) % 100 == 0:
            insert_postgress(stack, cursor, engine)
        
        
        
        
    except (Exception, Error) as error : 
        print('Error al conectar con postgress', error)



   

#conectar a postgress y crear objeto cursor
try:
    engine = psycopg2.connect(
        database=DATABASE,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = engine.cursor()
   

   

except (Exception, Error) as error : 
    print('Error al conectar con postgress', error)
else:
    try:
        
        confc = {'bootstrap.servers': KAFKA_IP+':'+KAFKA_PORT,
            'group.id': "consumer3",
            'auto.offset.reset': 'latest',
            'enable.auto.commit':'false'}
        consumer = Consumer(confc)
        running = True
    except (Exception):
        print(Exception)
    else:
        stack=[]
        basic_consume_loop(stack,consumer,['pruebascrack'])
finally:
    if engine:
        cursor.close()
        engine.close()
        print("PostgreSQL connection is closed")
    if consumer:
        shutdown()
        consumer.close()
    
























