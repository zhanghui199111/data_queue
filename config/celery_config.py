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