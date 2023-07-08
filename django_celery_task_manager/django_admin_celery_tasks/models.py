from celery.result import AsyncResult
from django.db import models
from celery import Task
from django.contrib.postgres.fields import ArrayField, JSONField
from django.utils.functional import cached_property

from django.contrib.auth import get_user_model


class BaseCeleryTask(models.Model):

    task_id = models.CharField(max_length=100, default='-')

    created_at = models.DateTimeField(auto_now_add=True)

    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    state_persistent = models.CharField(max_length=100, default='')

    logs = models.TextField(max_length=1000000, null=True, blank=True)

    exception_trace_persistent = models.TextField(max_length=1000000, null=True, blank=True)
    result_persistent = JSONField(null=True, blank=True)


    run_by = models.ForeignKey(to=get_user_model(), null=True, blank=True, on_delete=models.SET_NULL)

    warnings = models.TextField(max_length=1000000, null=True, blank=True)

    @classmethod
    def get_readonly_fields(cls):
        return [
            f.name
            for f in cls._meta.get_fields()
            if f.name not in cls.get_parameters_fields()
        ] + ['exception_trace', 'state', 'progress_percent', 'result']

    @cached_property
    def task_result(self):
        return AsyncResult(self.task_id)

    @property
    def exception_trace(self):
        return self.exception_trace_persistent or self.task_result.traceback or '-'

    @property
    def state(self):
        return self.state_persistent or self.task_result.state or '-'

    @property
    def result(self):
        return self.result_persistent or self.task_result.result or '-'

    @property
    def progress_percent(self):

        info = self.task_result.info
        if not info:
            return '-'

        if self.task_result.state == 'PROGRESS':
            return round(
                100 * info.get('current', 1) / info.get('total', 1)
            )

        if self.task_result.state == 'SUCCESS':
            return 100

        return '-'

    def get_parameters(self):
        return {
            f: getattr(self, f)
            for f in self.get_parameters_fields()
        }

    @classmethod
    def get_parameters_fields(cls):
        raise NotImplementedError

    @staticmethod
    def get_task_func():
        raise NotImplementedError

    def run_task(self):

        from django.core import serializers

        r = self.get_task_func().apply_async(
            kwargs=self.get_parameters(),
            headers={'task_model_obj': serializers.serialize('json', [self], fields=['pk'])},
            countdown=1
        )
        self.task_id = r.id
        self.save()

    def __str__(self):
        return '%s #%s' % (self.__class__.__name__, self.pk)

    class Meta:
        abstract = True
