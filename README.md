### 一、大体框架
##### 大致框架如下图所示，data_queue为数据队列，用于存储数据。get_data为数据提供接口，get_data获取数据时，将数据存储到task_queue中。put_back数据放回模块，负责将task_queue中超时的数据放回data_queue中。以上几个模块协同工作，实现了一个简单的数据队列。
![avatar](https://raw.githubusercontent.com/zhanghui199111/Graph/master/Redis_data_queue.png)
### 二、data_queue模块
##### data_queue采用redis的list实现，用于储存数据，task_queue用于存储代码消费队列中的数据，使用redis的sorted set实现，具体代码如下所示：
```python
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
```
- __new__方法用于实现单例模式，不过这个类实现了一个多单例模式，相同task_name共用一个实例；
- __init__方法在初始化的时候初始化两个队列，data_queue与task_queue；
- insert_data方法将数据写入data_queue中；
- pop_data方法从data_queue中取出一个value，并且计算出该value的md5值，并将其与当前的时间戳结合存到task_queue中；
- delete_task方法将一个任务从task_queue中删除；
- putback_data方法将task_queue队列中时间超过delta_time的数据找出来，从右边写入data_queue中；
- check_task方法去task_queue队列中查询是否该任务还在task_queue中，如果存在则给其时间戳加上10，这样做的目的是防止delete的时候该task被放回data_queue中；
##### Redis类继承redis.StrictRedis，主要对从redis取出的数据做了编码处理，代码如下：
```python
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
```
### 三、Validation模块
##### Validation类用于对该redis_queue做验证，主要流程为插入一定量的数据，取出数据（每两个数中有一个未处理成功的情况）计入log，从log中取出数据来验证执行结果是否正确，具体代码如下：
```python
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
```
- insert_data向data_queue中插入0~num的数字；
- comsumer方法从data_queue中取出data出来消费，且每两个中有一个处理失败，该数据5s被putback_data方法从task_queue中取出放回data_queue中；
- consumer_multi方法与consumer方法类似，不同的是该方法通过redis的pipeline一次取出多个数据，而consumer每次只取出一个数据；
- validation方法对consumer或者consumer_multi消费的数据进行验证，看是不是从0~num，如果是0~num，则表示整个数据消费过程是正确的；
### 四、整体流程运转
##### 整个工程建立在celery的框架上，通过celery的定时任务每5分钟选取task_queue中超时的数据，将其放回data_queue中，celery配置如下：
```python
from datetime import timedelta

CELERY_RPC_BACKEND = 'amqp://user:password@host/rpc_backend'
BROKER_URL = 'amqp://user:password@host/broker'
CELERY_IMPORTS = ('modules.putback_data',
                  )

CELERY_ENABLE_UTC = False
CELERY_TIMEZONE = 'Asia/Shanghai'
CELERYBEAT_SCHEDULE = {
    'putback_data': {
        'task': 'modules.putback_data.putback',
        'schedule': timedelta(seconds=300)
    }
}
```
### 五、代码
> [https://github.com/zhanghui199111/data_queue](https://github.com/zhanghui199111/data_queue)