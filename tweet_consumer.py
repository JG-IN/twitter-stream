from datetime import timedelta
from redisqueue import RedisQueue
from pymongo import MongoClient
import faust
import json

app = faust.App(
    'retweet',
    broker=['kafka://server1:9092', 'kafka://server2:9092', 'kafka://server3:9092'],
    value_serializer='json',
    topic_partitions=2
)

def call_back(key, events):
    global table_elements_cnt
    global table_length
    
    if table_elements_cnt == 0:
        table_length = hashtag_summary_table.__sizeof__()
        queue.delete(queue.key)
        queue.put({key[0]: events})
        table_elements_cnt += 1

    elif table_elements_cnt == table_length - 1:
        queue.put({key[0]: events})
        table_elements_cnt, table_length = 0, 0

    else:
        queue.put({key[0]: events})
        table_elements_cnt += 1
    
hashtag_summary_table = app.Table('tweet', default=int, partitions=4,
                        use_partitioner=True, on_window_close=call_back).\
                        tumbling(timedelta(seconds=10), timedelta(seconds=10))

table_elements_cnt, table_length = 0, 0
tweet_topic = app.topic('kafka.tweets')

@app.agent(tweet_topic)
async def aggreagte_hash_tag(stream):
    async for event in stream:
        is_send = True
        created_at, user, hash_tags = event['created_at'], event['user']['name'], []
        keys = event.keys()
        key = ""

        # retweet case
        if 'retweeted_status' in keys :
            # 팔로우 목록에 존재하지 않는 유저
            if event['user']['id_str'] not in follow_list:
                is_send = False
            envent = event['retweeted_status']

        # reply case
        elif event['in_reply_to_user_id_str'] is not None:
            # 팔로우 목록에 존재하지 않는 유저
            if event['user']['id_str'] not in follow_list:
                is_send = False
        
        # quote case
        elif event['is_quote_status']:
            # 팔로우 목록에 존재하지 않는 유저
            if event['user']['id_str'] not in follow_list:
                is_send = False
            event = event['quoted_status']
        
        
        # 생략됐나 확인
        for hashtag in event['entities']['hashtags']:
            key = hashtag['text']
            if key != '':
                hashtag_summary_table[key] += 1
                hash_tags.append(key)

        # 팔로우에 없으면 저장 안 함
        if is_send:
            text = event['extended_tweet']['full_text'] if event['truncated'] else event['text']
            quote_count, reply_count, retweet_count, favorite_count = \
                event['quote_count'], event['reply_count'], event['retweet_count'], event['favorite_count']
            url = event['entities']['urls'][0]['url'] if len(event['entities']['urls']) else ''
            data = {
                'created_at': created_at,
                'user': user,
                'text': text,
                'hash_tags': hash_tags,
                'quote_count': quote_count,
                'reply_count': reply_count,
                'retweet_count': retweet_count,
                'favorite_count': favorite_count,
                'url': url
            }
            print('store mongodb')
            tweets.insert_one(data)
        
        # if key != "":
        #     print('key : {}, freq : {}'.format(key, hashtag_summary_table[key].current()))    
       
if __name__ == "__main__":
    with open('./follow_list_id2name.json', 'r') as file:
        follow_list = list(json.load(file))
    
    client = MongoClient(host='192.168.1.37', port=27017)
    database = client.tweet_stream

    tweets = database.tweets

    queue = RedisQueue('hashtag_trend', host='192.168.1.37', port=6379, db=0)

    app.main()


        
