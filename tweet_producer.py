from tweepy import OAuthHandler, Stream
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging

class TwitterStreamListener(Stream):
    def on_status(self, raw_data):
        raw_data = raw_data._json

        # If user key is not in key list
        if 'user' not in raw_data.keys():
            logger.error('Unknown message type: %s', raw_data)

        try:
            producer.send_data('kafka.tweets', raw_data)
        except KafkaError as e:
            print(e)
            return False

    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            return False

class Producer():
    def __init__(self, bootstrap_servers):
        print("Kafka Producer Object Create")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers = bootstrap_servers,
                max_block_ms = 10000,
                retries = 0,
                acks = 1,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        except KafkaError as exc:
            print('kafka producer - Exception during connecting to broker - {}'.format(exc))
            return False
        
    def stop(self):
        print("Kafka Producer Object Stop")
        self.producer.close()
    
    def send_data(self, topic, data):
        future = self.producer.send(topic, data).add_callback(self.on_send_success).add_errback(self.on_send_error)
        print(future)
        self.producer.flush()
    
    def on_send_success(self, record_metadata):
        print("**********Send Success***********")
        print("record_metadata.topic: ", record_metadata.topic)
        print("record_metadata.partition: ", record_metadata.partition)
        print("record_metadata.offset: ", record_metadata.offset)
        pass
    
    def on_send_error(self, excp):
        print("**********Send Error Occur**********")
        log.error("I am an errback", exc_info=excp)

logging.basicConfig(
    filename="error.log",
    filemode='w', 
    format='%(levelname)s %(asctime)s - %(message)s',
    level=logging.ERROR
)

logger = logging.getLogger()

producer = Producer(bootstrap_servers=['server1:9092', 'server2:9092', 'server3:9092'])

with open('./follow_list.json', 'r') as file:
    follow_list = list(json.load(file).values())


auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)


stream = TwitterStreamListener(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
stream.filter(follow=follow_list)


