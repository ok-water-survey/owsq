from celery.task import task
from celery.task.sets import subtask
from celery import chord
from pymongo import Connection
from datetime import datetime,timedelta
#from cybercom.data.catalog import datacommons #catalog

mongoHost = 'localhost'


@task()
def add(x, y):
    return x + y

@task()
def tsum(numbers):
    return sum(numbers)
@task()
def data_workflow(num, **kwargs):
    callback = tsum.subtask()
    header = [add.subtask((i, i)) for i in xrange(int(num))]
    return chord(header)(callback).get()
