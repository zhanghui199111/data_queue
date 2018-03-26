from datetime import timedelta

CELERY_RPC_BACKEND = 'amqp://user:password@host/rpc_backend'
BROKER_URL = 'amqp://user:password@host/broker'
CELERY_IMPORTS = ('data.put_data_to_queue',
                  'data.putback_data'
                  )

CELERY_ENABLE_UTC = False
CELERY_TIMEZONE = 'Asia/Shanghai'
CELERYBEAT_SCHEDULE = {
    'put_data_to_queue': {
        'task': 'data.put_data_to_queue,main',
        'schedule': timedelta(seconds=60)
    },
    'putback_data': {
        'task': 'celery_redis.putback_data.putback_task',
        'schedule': timedelta(seconds=300)
    }
}