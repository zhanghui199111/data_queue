from data.redis_class import QueueRedis


def save_data_to_queue(queue_redis):
    for i in range(2000):
        queue_redis.lpush("data_queue", i)


def main():
    queue_redis = QueueRedis(host="host", password="password")
    save_data_to_queue(queue_redis)


if __name__ == "__main__":
    main()
