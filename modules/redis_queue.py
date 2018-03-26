# -*- coding: utf-8 -*-
import os
import time
import hashlib
import json
from collections import defaultdict
from utils.redis_class import (Redis,
                               )
from multiprocessing import Process


class RedisQueue(object):
    _instance = defaultdict()

    def __new__(cls, *args, **kw):
        if not cls._instance.get(args[0], None):
            cls._instance[args[0]] = super(RedisQueue, cls).__new__(cls)
        return cls._instance[args[0]]

    def __init__(self, task_name):
        self.data_queue = "%s_data_queue" % (task_name,)
        self.task_queue = "%s_task_queue" % (task_name,)

        self.queue_redis = Redis(host='localhost', password=None, db=1)
        self.cache_redis = Redis(host='localhost', password=None, db=2)

    def get_string_md5(self, string):
        """
        :param string: string value
        :return: md5 value of string
        """
        m = hashlib.md5()
        m.update(string.encode('utf-8'))
        return m.hexdigest()

    def insert_data(self, value):
        self.queue_redis.lpush(self.data_queue, json.dumps(value))

    def pop_data(self):
        if not self.queue_redis.exists(self.data_queue):
            # logger.info("data_queue do not exists!")
            return None, None
        else:
            value_str = self.queue_redis.rpop(self.data_queue)
            if value_str is not None:
                value = json.loads(value_str)
                key = self.get_string_md5(str(value))
                self.record_task(key)
                self. cache_redis.set(key, value_str)
                return key, value
            else:
                return None, None

    def pop_data_num(self, num):
        '''
        通过pipeline一次获取多个数据
        :param num:获取的数量
        :return:结果list
        '''
        if not self.queue_redis.exists(self.data_queue):
            return []
        else:
            with self.queue_redis.pipeline() as queue_redis_pipeline:
                for i in range(num):
                    queue_redis_pipeline.rpop(self.data_queue)
                result = queue_redis_pipeline.execute()

        key_value_list = []
        for value_str in result:
            if value_str is not None:
                value = json.loads(value_str)
                key = self.get_string_md5(str(value))
                self.record_task(key)
                self.cache_redis.set(key, value)
                key_value_list.append((key, value))
        return key_value_list

    def record_task(self, key):
        current_timestamp = time.time()
        self.queue_redis.zadd(self.task_queue, current_timestamp, key)

    def delete_task(self, key):
        self.queue_redis.zrem(self.task_queue, key)
        self.cache_redis.delete(key)

    def putback_task(self, delta_time):
        current_timestamp = time.time()
        putback_keys = self.queue_redis.zrangebyscore(self.task_queue, 0, current_timestamp - delta_time)
        for key in putback_keys:
            value = self.cache_redis.get(key)
            self.queue_redis.rpush(self.data_queue, value)
            self.delete_task(key)
        return len(putback_keys)

    def check_task(self, key):
        result = self.queue_redis.zrank(self.task_queue, key)
        if result is not None:
            self.queue_redis.zincrby(self.task_queue, key, 10)
            return True
        else:
            return False


class Validation(object):

    def __init__(self):
        self.instance = RedisQueue("test")

    def insert_data(self, num):
        for i in range(num):
            self.instance.insert_data(i)

    def consumer(self, filename):
        result = open(filename, "w")
        i = 0
        while self.instance.queue_redis.exists(self.instance.data_queue) or self.instance.queue_redis.exists(self.instance.task_queue):
            self.instance.putback_task(5)
            key, value = self.instance.pop_data()
            if value is None:
                continue
            if i % 2 == 0:
                result.write("%d\n" % (value,))
                if self.instance.check_task(key):
                    self.instance.delete_task(key)
            else:
                # print "failure: ", data
                pass
            i += 1
        result.close()

    def consumer_multi(self, filename):
        result = open(filename, "w")
        i = 0
        while self.instance.queue_redis.exists(self.instance.data_queue) or self.instance.queue_redis.exists(self.instance.task_queue):
            self.instance.putback_task(5)
            key_value_list = self.instance.pop_data_num(10)
            for (key, value) in key_value_list:
                if value is None:
                    continue
                if i % 2 == 0:
                    result.write("%d\n" % (value,))
                    if self.instance.check_task(key):
                        self.instance.delete_task(key)
                else:
                    pass
                i += 1
        result.close()

    def validation(self, filename, num):
        f = open(filename, "r")
        result = []
        for line in f.readlines():
            result.append(int(line.strip()))
        result.sort()
        print(result)
        for i in range(0, num):
            if i != result[i]:
                print("error")
        print("success")


def main():
    num = 20000
    filename = "result.log"
    validation = Validation()
    validation.insert_data(num)
    validation.consumer_multi(filename)
    validation.validation(filename, num)
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
    # consumer(instance)
    # validation()

    # import pprint
    # pprint.pprint(instance.pop_data())


if __name__ == "__main__":
    main()
