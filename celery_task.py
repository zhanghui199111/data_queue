from celery import Celery

app = Celery('celery_app')
app.config_from_object('config.celery_config')

if __name__ == '__main__':
    app.start()
