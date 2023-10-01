from re import findall
from copy import deepcopy
from django.db import models
from django.utils.text import capfirst
from asyncio import iscoroutinefunction
from asgiref.sync import sync_to_async
from rest_framework.reverse import reverse
from rest_framework.response import Response
from rest_framework.validators import UniqueValidator

getattr, reverse, deepcopy = sync_to_async(getattr), sync_to_async(reverse), sync_to_async(deepcopy)
findall = sync_to_async(findall)


async def to_coroutine(function):
    if not iscoroutinefunction(function):
        function = sync_to_async(function)
    return function


async def get_field_info(obj):
    fields, forward_relations = {}, {}
    if hasattr(obj.__class__, '_meta'):
        for field in obj.__class__._meta.get_fields(include_parents=False):
            data = fields if not field.remote_field else forward_relations
            if not field.remote_field or not field.auto_created:
                data[field.name] = {}
    
    return {'fields': fields, 'forward_relations': forward_relations}


async def get_errors_formatted(serializer):
    errors_remplate = {"jsonapi": { "version": "1.1" }, 'errors': []}
    if not hasattr(serializer, '_errors'):
        msg = 'You must call `.is_valid()` before accessing `.errors`.'
        raise AssertionError(msg)
    elif serializer._errors.get('errors', None):
        errors_remplate['errors'] = serializer._errors
        return errors_remplate
    error_details = []
    for key, val in serializer._errors.items():
        url = await getattr(serializer, serializer.url_field_name, None)
        error, error_detail = None, {'code': 403}
        if type(val) == dict:
            error = val
        else:
            key = 'type' if key == 'type.type' else key
            error = {key: val}
        if not error:
            continue
        if url:
            error_detail['source'] = {'pointer': url}
        error_detail['detail'] = "The JSON field {0}caused an exception: {1}".format(
            "\"" + key + "\" ", error[key][0].lower()
        )
        error_details.append(error_detail)
    if not error_details:
        return None
    serializer._errors = {"jsonapi": { "version": "1.1" }, 'errors': error_details}
    return serializer._errors


async def get_type_from_model(obj_type):
    return '-'.join(await findall('[A-Z][^A-Z]*', obj_type.__name__)).lower()


async def get_related_field(queryset, kwargs):
    object = await queryset.aget(id=kwargs['pk'])
    try:
        field_name = kwargs['field_name']
        field = await getattr(object, field_name)
    except AttributeError:
        return Response({'data': None}, status=404)
    else:
        return field

async def get_related_field_objects(field):
    try:
        field = [obj async for obj in field.all()]
    except (AttributeError, TypeError):
        field = [field] if field else []
    return field


def get_relation_kwargs(field_name, relation_info):
    """
    Creates a default instance of a flat relational field.
    """
    def needs_label(model_field, field_name):
        default_label = field_name.replace('_', ' ').capitalize()
        return capfirst(model_field.verbose_name) != default_label


    def get_unique_error_message(model_field):
        unique_error_message = model_field.error_messages.get('unique', None)
        if unique_error_message:
            unique_error_message = unique_error_message % {
                'model_name': model_field.model._meta.verbose_name,
                'field_label': model_field.verbose_name
            }
        return unique_error_message
    
    model_field, related_model, to_many, to_field, has_through_model, reverse = relation_info
    kwargs = {
        'queryset': related_model._default_manager,
        'view_name': '-'.join(findall.func('[A-Z][^A-Z]*', related_model.__name__)).lower() + '-detail'
    }

    if to_many:
        kwargs['many'] = True

    if to_field:
        kwargs['to_field'] = to_field

    limit_choices_to = model_field and model_field.get_limit_choices_to()
    if limit_choices_to:
        if not isinstance(limit_choices_to, models.Q):
            limit_choices_to = models.Q(**limit_choices_to)
        kwargs['queryset'] = kwargs['queryset'].filter(limit_choices_to)

    if has_through_model:
        kwargs['read_only'] = True
        kwargs.pop('queryset', None)

    if model_field:
        if model_field.verbose_name and needs_label(model_field, field_name):
            kwargs['label'] = capfirst(model_field.verbose_name)
        help_text = model_field.help_text
        if help_text:
            kwargs['help_text'] = help_text
        if not model_field.editable:
            kwargs['read_only'] = True
            kwargs.pop('queryset', None)
        if model_field.null:
            kwargs['allow_null'] = True
        if kwargs.get('read_only', False):
            # If this field is read-only, then return early.
            # No further keyword arguments are valid.
            return kwargs

        if model_field.has_default() or model_field.blank or model_field.null:
            kwargs['required'] = False
        if model_field.validators:
            kwargs['validators'] = model_field.validators
        if getattr.func(model_field, 'unique', False):
            validator = UniqueValidator(
                queryset=model_field.model._default_manager,
                message=get_unique_error_message(model_field))
            kwargs['validators'] = kwargs.get('validators', []) + [validator]
        if to_many and not model_field.blank:
            kwargs['allow_empty'] = False

    return kwargs
