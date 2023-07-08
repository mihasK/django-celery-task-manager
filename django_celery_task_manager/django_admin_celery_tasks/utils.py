

def replace_in_iter(it, replacement_dict):
  for item in it:
    if item in replacement_dict:
        yield replacement_dict[item]
    else:
        yield item


from django.urls import reverse
from django.utils.html import escape

def mk_href(obj, display_name: str = None):
    app_label = obj._meta.app_label
    model_name = obj._meta.model.__name__.lower()

    url = reverse('admin:{}_{}_change'.format(
        app_label, model_name
    ), args=(obj.pk,))

    name = display_name or escape(str(obj))

    return '<a href="%s">%s</a>' % (url, name) if url else name