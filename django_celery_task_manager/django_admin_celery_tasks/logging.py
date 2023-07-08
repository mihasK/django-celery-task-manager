import logging


from celery.utils.log import get_task_logger
from django.db.models import Value, F
from django.db.models.functions import Concat

from . models import BaseCeleryTask


class TaskModelHandler(logging.Handler):
    task_model_class = BaseCeleryTask
    task_model_id = None

    def __init__(self, task_model_class, task_model_id, level=logging.NOTSET):

        self.task_model_class = task_model_class
        self.task_model_id = task_model_id

        super().__init__(level)

    def emit(self, record):

        msg = '\n%s' % self.format(record)
        self.task_model_class.objects.filter(id=self.task_model_id).update(logs=Concat('logs', Value(msg)))


        if record.levelno >= logging.WARNING:
            self.task_model_class.objects.filter(id=self.task_model_id).update(warnings=Concat('warnings', Value(msg)))


def create_logger(task_model_obj: BaseCeleryTask):

    logger = get_task_logger(str(task_model_obj))

    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    # optionally logging on the Console as well as file
    # stream_handler = logging.StreamHandler()
    # stream_handler.setFormatter(formatter)
    # stream_handler.setLevel(logging.INFO)
    # # Adding File Handle with file path. Filename is task_id

    h = TaskModelHandler(task_model_class=task_model_obj.__class__,
                         task_model_id=task_model_obj.pk
                         )
    h.setFormatter(formatter)
    h.setLevel(logging.INFO)
    logger.addHandler(h)

    return logger