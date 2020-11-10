# TODO

import asyncio
from confluent_kafka import Consumer

topic_name = 'police.department.service.calls'

BROKER_URL = 'localhost:9092'

async def consume(topic_name):
    '''Consumes messages from a given Kafka topic'''
    
    c = Consumer({'bootstrap.servers' : BROKER_URL, 
                  'group.id' : 0,
                  "default.topic.config": {'auto.offset.reset': 'earliest'}
                 })
    
    c.subscribe([topic_name])
    
    while True:
        
        message = c.poll(timeout=1.0)
        
        if message is None:
            print('No Message Received')
            
        elif message.error() is not None:
            print('Error: {}'.format(message.error()))
            
        else:
            print(f' Key: {message.key()}, Value {message.value()}')
            
        await asyncio.sleep(0.1)
        
def main():
    
    try:
        asyncio.run(consume(topic_name))
    
    except KeyboardInterrupt as e:
        print('Shutting down')
        
if __name__ == '__main__':
    main()