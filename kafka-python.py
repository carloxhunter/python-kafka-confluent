from kafka import KafkaConsumer
#from pymongo import MongoClient
import psycopg2
from psycopg2 import Error
from json import loads

#conectar a postgress y crear objeto cursor
try:
    engine = psycopg2.connect(
        database="postgres",
        user="digevo",
        password="Digevobd*",
        host="database-omia.ccco8vwbpupr.us-west-2.rds.amazonaws.com",
        port='5432'
    )
    print("PostgreSQL server information")
    print(engine.get_dsn_parameters(), "\n")
    cursor = engine.cursor()
    cursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
    for table in cursor.fetchall():
        print(table)
    #crear tabla (solo primera vez)
    """ create_table_query = '''CREATE TABLE kafka_lc_dev
          (ID INT PRIMARY KEY NOT NULL,
          timestamp TIMESTAMP NOT NULL,
          kmsg_id VARCHAR(50),
          camera_id bigint,
          frame_init bigint,
          frame_fin bigint,
          peroid bigint,
          obj_type smallint,
          line_id smallint,
          count smallint
        ); '''
    cursor.execute(create_table_query)
    engine.commit()  """

    cur = engine.cursor()
    q = """                              
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    where table_name = 'kafka_lc_dev';
    """
    cur.execute(q)  # (table_name,) passed as tuple
    print(cur.fetchall())

    #for keys in cur.fetchall():
    #        print(keys)

except (Exception, Error) as error : print('Error al conectar con postgress', error)
else:
    #print('jiro')
    try:
        consumer = KafkaConsumer(
                'quickstart-events',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='my-group',
                value_deserializer=lambda x: loads(x.decode('utf-8')))

        for message in consumer:
            message = message.value
            #collection.insert_one(message)
            print('{}'.format(message))
    except(): print('error al leer mensajes')