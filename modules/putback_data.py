from celery_task import app
from modules.redis_queue import RedisQueue


@app.task
def putback():
    redis_queue = RedisQueue("test")
    redis_queue.putback_task(300)
