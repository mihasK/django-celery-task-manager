from django.contrib import admin

from datetime import datetime

import easy
import humanize
from admin_object_actions.admin import ModelAdminObjectActionsMixin
from admin_object_actions.forms import AdminObjectActionForm
from django.contrib import admin, messages
from django.contrib.admin.models import LogEntry
from django.contrib.admin.widgets import AutocompleteSelect
from django.contrib.contenttypes.models import ContentType
from django.urls import reverse
from django.utils.html import format_html
from django.utils.safestring import mark_safe
from clientdashboardservices.utils import mk_href

from django_admin_celery_tasks.models import BaseCeleryTask
from django_admin_celery_tasks.utils import replace_in_iter
from django import forms
from django.contrib import messages

from crum import get_current_request, get_current_user

def get_repeat_form(task_model_class):
    class RepeatForm(AdminObjectActionForm):
        class Meta:
            model = task_model_class
            fields = task_model_class.get_parameters_fields()

        def do_object_action(self):
            new_imp = self.instance.__class__.objects.create(
                **{
                    f: getattr(self.instance, f)
                    for f in self.instance.__class__.get_parameters_fields()
                },
                run_by=get_current_user()
            )
            new_imp.run_task()

            LogEntry.objects.log_action(
                user_id=get_current_user().pk,
                content_type_id=ContentType.objects.get_for_model(task_model_class).id,
                object_id=new_imp.pk,
                object_repr=str(new_imp),
                action_flag=3,  # CREATE,
                change_message='Task object was created (the task repeated with the same parameters)'
            )
            messages.success(get_current_request(), mark_safe("New task created: %s" % mk_href(new_imp)))

    return RepeatForm

class CeleryTaskAdminMixin(ModelAdminObjectActionsMixin, admin.ModelAdmin):

    task_state_fields_additional = []


    date_hierarchy = 'started_at'

    TASK_MODEL = None

    list_display = ('id', 'started_at', 'time',
                    'state',
                    'run_by_',
                    'display_object_actions_list',
    )

    list_filter = (
        ('run_by', admin.RelatedOnlyFieldListFilter),
    )

    run_by_ = easy.ForeignKeyAdminField('run_by', short_description='Run by')

    def __init__(self, *args, **kwargs) -> None:
        super().__init__( *args, **kwargs)
        if not self.TASK_MODEL:
            raise NotImplementedError(
                'TASK_MODEL (subclass of BaseCeleryTask) should be defined for admin class.'
            )
        assert issubclass(self.TASK_MODEL, BaseCeleryTask)

    def time(self, obj):
        if obj.finished_at and obj.started_at:
            return humanize.naturaldelta((obj.finished_at - obj.started_at).total_seconds())
        elif obj.state in ('SUCCESS', 'FAILURE'):  # If task is finished, but finished_at has not been recorded for s.r.
            return '-'
        elif obj.started_at:
            return humanize.naturaldelta(datetime.now().replace(tzinfo=None) - obj.started_at.replace(tzinfo=None))
        else:
            '-'

    @easy.with_tags()
    def warnings_(self, obj: BaseCeleryTask):
        text = obj.warnings
        # text = str(text)
        text = text.replace('\n', '<br>')
        
        assert isinstance(text, str)
        return '<font color="orange"> %s </font>' % str(text)

    @easy.with_tags()
    def exception_trace_(self, obj: BaseCeleryTask):
        text = obj.exception_trace
        text = text.replace('\n', '<br>')
        return text
        # return format_html(
        #     '<font color="{color}"> {text} </font>',
        #     text=text, color='red'
        # )


    def state(self, obj: BaseCeleryTask):

        msg = '{state} ({percent}%)'.format(state=obj.state.title().replace('_', ' '), percent=obj.progress_percent)

        color = 'orange'
        if  obj.state == 'SUCCESS':
            color = 'green'
        elif obj.state == 'FAILURE':
            color = 'red'

        return format_html('<font color="{color}"> {text} </font>'.format(text=msg, color=color))
    state.admin_order_field = 'state_persistent'

    change_form_template = 'celery_task_change_template.html'

    def get_readonly_fields(self, request, obj=None):
        if obj:
            res = tuple(self.TASK_MODEL.get_parameters_fields()) + \
                  tuple(self.TASK_MODEL.get_readonly_fields()) + \
                  ('display_object_actions_detail',)
        else:
            res =  tuple(self.TASK_MODEL.get_readonly_fields() + ['display_object_actions_detail', ])


        res = list(replace_in_iter(
            res,
            self.REPLACEMENT_FIELDS_DICT
        )) + list(self.task_state_fields_additional)

        return res

    REPLACEMENT_FIELDS_DICT = {
        'warnings': 'warnings_',
         'exception_trace': 'exception_trace_',
        'run_by': 'run_by_',
    }
    
    
    def get_fieldsets(self, request, obj=None):
        return  (
            (None, {
                'fields': ('display_object_actions_detail',)
            }) ,
          ('Parameters', {
              'fields': tuple(self.TASK_MODEL.get_parameters_fields())
          }),
          ('State', {
              'fields': list(replace_in_iter(
                  [
                      f for f in self.TASK_MODEL.get_readonly_fields()
                      if 'persistent' not in f
                  ],
                  self.REPLACEMENT_FIELDS_DICT

              )) + list(self.task_state_fields_additional)
          }),
        )

    def get_queryset(self, request):
        return super().get_queryset(request).prefetch_related('run_by')

    def changeform_view(self, request, object_id=None, form_url='', extra_context=None):

        if object_id:
            extra_context = extra_context or dict()
            extra_context['read_only'] = True

        return super().changeform_view(request, object_id, form_url, extra_context)

    def save_model(self, request, obj, form, change):
        obj.run_by = get_current_user()
        super().save_model(request, obj, form, change)
        obj.run_task()

    def formfield_for_dbfield(self, db_field, request, **kwargs):
        return super().formfield_for_dbfield(db_field, request, **kwargs)

    @property
    def object_actions(self):
        return  [
        {
            'slug': 'repeat',
            'verbose_name': 'Repeat the task',
            'verbose_name_title': 'Repeat',
            'verbose_name_past': 'repeated',
            'form_class': get_repeat_form(task_model_class=self.TASK_MODEL),
            'fields': ('id', ) + self.TASK_MODEL.get_parameters_fields(),
            'readonly_fields': ('id', ) ,
            'permission': 'view',
            'fieldsets': (
                ('The task which we repeat', {
                    'fields': ('id',),
                }),
                ('Parameters', {
                    'fields': self.TASK_MODEL.get_parameters_fields()
                })
            )
        },
    ]



    def has_change_permission(self, request, obj=None):
        return False


