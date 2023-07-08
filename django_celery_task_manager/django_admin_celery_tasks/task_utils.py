import datetime
import logging
from celery import signals, Task
from django.core import serializers
from .models import BaseCeleryTask
from .logging import create_logger

def get_task_model_obj(celery_task) -> BaseCeleryTask:
    if not hasattr(celery_task.request, 'task_model_obj'):
        return None

    s = next(
        serializers.deserialize('json', celery_task.request.task_model_obj)
    )
    task_obj = s.object
    return task_obj.__class__.objects.get(pk=task_obj.pk)

from django.utils import timezone



def before_task_run(*args, **kwargs):

    celery_task = kwargs['task']
    task_model_obj = get_task_model_obj(celery_task)

    if not task_model_obj: # Task was invoked not via model creation, but rather directly
        celery_task.logger = logging  # specify some default logger 
        print('Task was invoked directly, skipping model-related setup.')
        return  

    celery_task.logger = create_logger(task_model_obj)

    r = task_model_obj.__class__.objects.filter(pk=task_model_obj.pk).update(
        started_at=timezone.now()
    )
    # print('Before task (%s): %s objects updated (1 is expected in case of correct execution)' % (celery_task, r))
    assert r == 1


def after_task_run(*args, **kwargs):


    celery_task = kwargs['sender']
    task_model_obj = get_task_model_obj(celery_task)

    if not task_model_obj:
        print('Task was invoked directly, skipping model-related post actions.')

        return  # Task was invoked not via model creation, but rather directly



    result_status = 'SUCCESS' if kwargs['signal'].name == 'task_success' else 'FAILURE'
    if result_status == 'SUCCESS':
        if task_model_obj.warnings:
            result_status = 'FINISHED_WITH_WARNINGS'


    r = task_model_obj.__class__.objects.filter(pk=task_model_obj.pk).update(
        finished_at=timezone.now(),
        state_persistent=result_status,
        result_persistent=kwargs.get('result', ''),
        exception_trace_persistent=str(kwargs.get('einfo', '')),
    )
    # print('After task (%s) : %s objects updated (1 is expected in case of correct execution)' % (celery_task, r))
    assert r == 1


def connect_signals(task):
    signals.task_prerun.connect(before_task_run, task)
    signals.task_success.connect(after_task_run, task)
    signals.task_failure.connect(after_task_run, task)






def update_progress(celery_task: Task, current: int, total: int):

    celery_task.logger.info('update_progress: %s out of  %s' % (current, total))
    celery_task.update_state(
        state='PROGRESS',
        meta={'current': current, 'total': total}
    )