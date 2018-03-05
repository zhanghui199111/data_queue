# -*- coding:utf-8 -*-
import os
import time
from data.redis_class import (QueueRedis,
                              DataRedis
                              )
from celery_proj import app

queue_redis = QueueRedis(host="host", password="password")
data_redis = DataRedis(host="host", password="password")


def delete_task(key):
    queue_redis.zrem("task_queue", key)
    data_redis.delete(key)


@app.task
def putback_task():
    current_timestamp = time.time()
    putback_keys = queue_redis.zrange_by_score("task_queue", 0, current_timestamp - 300)
    for key in putback_keys:
        value = data_redis.get(key)
        queue_redis.rpush("data_queue", value)
        delete_task(key)
    return len(putback_keys)
