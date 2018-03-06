# -*- coding: utf-8 -*-

import redis
import ast


class Redis(object):
    """
    单例模式
    """
    _instance = None

    def __new__(cls, *args, **kw):
        if not cls._instance:
            cls._instance = super(Redis, cls).__new__(cls)  # windows
        return cls._instance

    def __init__(self, host='localhost', port=6379, password=None, db=0):
        if password is not None:
            self.conn = redis.StrictRedis(host=host, port=port, password=password, db=db)
        else:
            self.conn = redis.StrictRedis(host=host, port=port, db=db)

    def set(self, key, value, expire_time=None):
        self.conn.set(key, value)
        if expire_time is not None:
            self.conn.expire(key, expire_time)

    def get(self, key):
        value = self.conn.get(key)
        if value is not None:
            return value.decode("utf-8")
        else:
            return value

    def delete(self, key):
        return self.conn.delete(key)

    def lpush(self, key, value, expire_time=None):
        self.conn.lpush(key, value)
        if expire_time is not None:
            self.conn.expire(key, expire_time)

    def rpush(self, key, value, expire_time=None):
        self.conn.rpush(key, value)
        if expire_time is not None:
            self.conn.expire(key, expire_time)

    def blpop(self, key):
        result = self.conn.blpop(key)
        if result is not None:
            return ast.literal_eval(result[1])
        else:
            return result

    def brpop(self, key):
        result = self.conn.brpop(key)
        if result is not None:
            return ast.literal_eval(result[1])
        else:
            return result

    def rpop(self, key):
        value = self.conn.rpop(key)
        if value is not None:
            return ast.literal_eval(value.decode("utf-8"))
        else:
            return value

    def zadd(self, key, score, member):
        return self.conn.zadd(key, score, member)

    def zrank(self, key, member):
        return self.conn.zrank(key, member)

    def zrem(self, key, member):
        return self.conn.zrem(key, member)

    def zrange_by_score(self, key, min, max):
        return [i for i in self.conn.zrangebyscore(key, min, max)]

    def zincrby(self, key, increment, member):
        return self.conn.zincrby(key, increment, member)

    def rpop_and_lpush(self, source, destination):
        return self.conn.rpoplpush(source, destination)

    def llen(self, key):
        return self.conn.llen(key)

    def exists(self, key):
        return self.conn.exists(key)

    def get_value(self, key):
        result = self.conn.get(key)
        if result is not None:
            return ast.literal_eval(result)
        else:
            return None


class QueueRedis(Redis):

    def __init__(self, host="localhost", port=6379, password=None, db=2):
        super(QueueRedis, self).__init__(host=host, port=port, password=password, db=db)


class DataRedis(Redis):

    def __init__(self, host="localhost", port=6379, password=None, db=3):
        super(DataRedis, self).__init__(host=host, port=port, password=password, db=db)
