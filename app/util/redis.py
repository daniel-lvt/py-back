import redis


class InstanceRedis:
    def __init__(self):
        self.redisIns = redis.Redis(host='localhost', port=6379, db=0)

    def get_instance_redis(self, name):
        return self.redisIns.get(name)

    def set_instance_redis(self, key, data):
        self.redisIns.set(key, data)
        return True
