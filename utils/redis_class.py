import redis


class Redis(redis.StrictRedis):

    # _instance = None
    #
    # def __new__(cls, *args, **kw):
    #     if not cls._instance:
    #         cls._instance = super(Redis, cls).__new__(cls)
    #     return cls._instance

    def __init__(self, host='localhost', port=6379, password=None, db=0):
        if password is not None:
            super(Redis, self).__init__(host=host, port=port, password=password, db=db)
        else:
            super(Redis, self).__init__(host=host, port=port, db=db)

    def get(self, key):
        return super(Redis, self).get(key).decode("utf-8")

    def rpop(self, key):
        value = super(Redis, self).rpop(key)
        if value is not None:
            return value.decode("utf-8")
        else:
            return value
