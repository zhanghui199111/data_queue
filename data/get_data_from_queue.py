# -*- coding: utf-8 -*-
import time
import hashlib
from data.redis_class import (QueueRedis,
                              DataRedis
                              )

queue_redis = QueueRedis(host="host", password="password")
data_redis = DataRedis(host="host", password="password")


class GetRedisData(object):
    _instance = None

    def __new__(cls, *args, **kw):
        if not cls._instance:
            cls._instance = super(GetRedisData, cls).__new__(cls)  # windows
        return cls._instance

    def __init__(self, data_queue, task_queue):
        self.data_queue = data_queue
        self.task_queue = task_queue

    def get_string_md5(self, string):
        """
        :param string: string value
        :return: md5 value of string
        """
        m = hashlib.md5('python'.encode('utf-8'))
        m.update(string.encode('utf-8'))
        return m.hexdigest()

    def pop_data(self):
        if not queue_redis.exists(self.data_queue):
            # logger.info("data_queue do not exists!")
            return None, None
        else:
            value = queue_redis.rpop(self.data_queue)
            if value is not None:
                key = self.get_string_md5(str(value))
                self.record_task(key)
                data_redis.set(key, value)
                return key, value
            else:
                return None, None

    def record_task(self, key):
        current_timestamp = time.time()
        queue_redis.zadd(self.task_queue, current_timestamp, key)

    def delete_task(self, key):
        queue_redis.zrem(self.task_queue, key)
        data_redis.delete(key)

    def putback_task(self):
        current_timestamp = time.time()
        putback_keys = queue_redis.zrange_by_score(self.task_queue, 0, current_timestamp - 5)
        for key in putback_keys:
            value = data_redis.get(key)
            queue_redis.rpush(self.data_queue, value)
            self.delete_task(key)
        return len(putback_keys)

    def check_task(self, key):
        result = queue_redis.zrank(self.task_queue, key)
        if result is not None:
            queue_redis.zincrby(self.task_queue, key, 10)
            return True
        else:
            return False


def consumer(instance):
    i = 0
    while True:
        instance.putback_task()
        key, value = instance.pop_data()
        if value is None:
            continue
        # print data
        print("user doing something.......")
        if i % 2 == 0:
            print('success: ', value)
            if instance.check_task(key):
                instance.delete_task(key)
        else:
            # print "failure: ", data
            pass
        i += 1


def validation():
    f = open("1.log", "r")
    result = []
    for line in f.readlines():
        if line.startswith("success"):
            result.append(int(line.split(":")[1].strip()))
    result.sort()
    for i in range(2000):
        if i != result[i]:
            print("error")
    print("success")


def main():
    instance = GetRedisData("data_queue", "task_queue")
    # p1 = Process(target=consumer, args=(instance,))
    # p2 = Process(target=consumer, args=(instance,))
    # p3 = Process(target=consumer, args=(instance,))
    # p4 = Process(target=consumer, args=(instance,))
    # p1.start()
    # p1.join()
    # p2.start()
    # p2.join()
    # p3.start()
    # p3.join()
    # p4.start()
    # p4.join()
    consumer(instance)
    # validation()


if __name__ == "__main__":
    main()
