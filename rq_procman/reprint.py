import redis

KEY_PREFIX = 'test:log:'
KEY_PREFIX_LAST = 'test:log:last:'
REDIS_HOST = 'localhost'
MAX_LINES = 1000
KEYS_TTL = 600

class RePrint(object):
    """ 
        Redis Log Printer 
    """
    def __init__(self, name):
        self.key = KEY_PREFIX + name
        self.key_last = KEY_PREFIX_LAST + name
        self.redis = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)
        
        self.redis.delete(self.key)
        self.redis.delete(self.key_last)

    def msg(self, msg):
        pipe = self.redis.pipeline()
        pipe.lpush(self.key, msg)
        pipe.ltrim(self.key, 0, MAX_LINES)
        pipe.incr(self.key_last)
        pipe.expire(self.key, KEYS_TTL)
        pipe.expire(self.key_last, KEYS_TTL)
        pipe.execute()
      
