import redis

class RedisQueue(object):
    def __init__(self, name, **redis_kwargs):
        self.key = name
        self.rq = redis.Redis(**redis_kwargs)
    
    def size(self):
        return self.rq.llen(self.key)

    def isEmpty(self):
        return self.size() == 0

    def put(self, element):
        print('key : {}'.format(self.key))
        print('element : {}'.format(element))

        self.rq.lpush(self.key, element)

    def get(self, isBlocking=False, timeout=None):
        if isBlocking:
            element = self.rq.brpop(self.key, timeout=timeout)
            element = element[1]
        
        else:
            element = self.rq.rpop(self.key)
        return element
    
    def get_without_pop(self):
        if self.isEmpty():
            return None
        element = self.rq.lindex(self.key, -1)
        return element
    
