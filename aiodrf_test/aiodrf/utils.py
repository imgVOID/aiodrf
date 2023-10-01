from re import sub
from django.core.exceptions import ImproperlyConfigured
from rest_framework.fields import Field
from rest_framework.utils import model_meta
from functools import cached_property
from asyncio import ensure_future

from .helpers import getattr


# TODO: to test all of the lookups
class JSONAPIFilter:
    def __init__(self, queryset, request):
        self.queryset = queryset
        self.request = request
        self.params = {}
    
    async def filter_queryset(self):
        return self.queryset.filter(**await self._get_params())
        
    async def _get_params(self):
        for key, val in self.request.query_params.items():
            if not key.startswith('filter['):
                continue
            key = key.split('[')[-1].replace(']', '')
            if '__' in key:
                split_key = key.split('__')
                key, lookup = split_key[0], '__' + split_key[1]
            else:
                lookup = '__in'
            try:
                is_relation = await getattr(self.queryset.model, key, None)
            except AttributeError:
                continue
            is_relation = bool(is_relation.field.remote_field)
            key = key + '__id' + lookup if is_relation else key + lookup
            if ',' not in val and lookup != '__in' and val.isnumeric():
                val = int(val)
            elif lookup == '__range':
                split = val.split(',')
                val = [split[0], split[1]]
            else:
                val = val.split(',')
            if type(val) == list and val[0]:
                self.params.update({
                    key: [int(obj) if obj.isnumeric() else obj for obj in val]
                })
            else:
                self.params.update({key: []})
        return self.params


class JSONAPISerializerRepr:
    def __init__(self, serializer, indent=1, force_many=None):
        self._serializer = serializer
        self._indent = indent
        self._force_many = force_many
    
    def __repr__(self):
        serializer, indent = self._serializer, self._indent
        ret = self._field_repr(serializer, self._force_many) + ':'
        indent_str = '    ' * indent
        if self._force_many:
            fields = serializer.child._declared_fields
        else:
            fields = serializer._declared_fields
        for field_name, field in fields.items():
            ret += '\n' + indent_str + field_name + ' = '
            required_string = '' if field.required else f'required={field.required}'
            if hasattr(field, '_declared_fields'):
                ret += self.__class__(field, indent + 1).__repr__().replace(
                    '()', f"({required_string})"
                )
            elif hasattr(field, 'child'):
                child = field.child
                if hasattr(child, '_declared_fields'):
                    ret += '{}({}child={})'.format(
                        field.__class__.__name__, required_string + ', ',
                        self.__class__(child, indent + 1).__repr__(),
                    )
                else:
                    ret += self._field_repr(child)
            elif hasattr(field, 'child_relation'):
                ret += self._field_repr(field.child_relation, force_many=field.child_relation)
            else:
                ret += self._field_repr(field)
        return ret
    
    @staticmethod
    def _smart_repr(value):
        value = repr(value)
        if value.startswith("u'") and value.endswith("'"):
            return value[1:]
        return sub(' at 0x[0-9A-Fa-f]{4,32}>', '>', value)
    
    @classmethod
    def _field_repr(cls, field, force_many=False):
        kwargs = field._kwargs
        if force_many:
            kwargs = kwargs.copy()
            kwargs['many'] = True
            kwargs.pop('child', None)
        arg_string = ', '.join([cls._smart_repr(val) for val in field._args])
        kwarg_string = ', '.join([
            '%s=%s' % (key, cls._smart_repr(val))
            for key, val in sorted(kwargs.items())
        ])
        if arg_string and kwarg_string:
            arg_string += ', '
        if force_many:
            class_name = force_many.__class__.__name__
        else:
            class_name = field.__class__.__name__
        return "%s(%s%s)" % (class_name, arg_string, kwarg_string)


class NotSelectedForeignKey(ImproperlyConfigured):
    def __init__(self, message=None):
        self.message = (
            'Model.objects.select_related(<foreign_key_field_name>, ' 
            '<foreign_key_field_name>__<inner_foreign_key_field_name>) '
            'must be specified.'
        )
        super().__init__(self.message)


class cached_property(cached_property):
    async def __get__(self, instance, owner=None):
        if instance is None:
            return self
        if self.attrname is None:
            raise TypeError(
                "Cannot use cached_property instance without calling __set_name__ on it.")
        try:
            cache = instance.__dict__
        except AttributeError:
            msg = (
                f"No '__dict__' attribute on {type(instance).__name__!r} "
                f"instance to cache {self.attrname!r} property."
            )
            raise TypeError(msg) from None
        if not cache.get(self.attrname):
            val = await ensure_future(self.func(instance))
            try:
                cache[self.attrname] = val
            except TypeError:
                msg = (
                    f"The '__dict__' attribute on {type(instance).__name__!r} instance "
                    f"does not support item assignment for caching {self.attrname!r} property."
                )
                raise TypeError(msg) from None
        else:
            val = cache[self.attrname]
        return val

class RaiseNested:
    errors = {
        'not_writtable_nested':
            'The `.{method_name}()` method does not support writable nested '
            'fields by default.\nWrite an explicit `.{method_name}()` method for '
            'serializer `{module}.{class_name}`, or set `read_only=True` on '
            'nested serializer fields.',
        'not_writtable_dotted-source':
            'The `.{method_name}()` method does not support writable dotted-source '
            'fields by default.\nWrite an explicit `.{method_name}()` method for '
            'serializer `{module}.{class_name}`, or set `read_only=True` on '
            'dotted-source serializer fields.'
    }
    
    def __init__(self, method_name, serializer, validated_data):
        self.method_name, self.serializer, self.validated_data = method_name, serializer, validated_data
        self.model_field_info = model_meta.get_field_info(serializer.Meta.model)
    
    async def raise_nested_writes(self):
        assert not any(
            isinstance(field, Field) and
            (field.source in self.validated_data) and
            (field.source in self.model_field_info.relations) and
            isinstance(self.validated_data[field.source], (list, dict))
            for field in await self.serializer._writable_fields
        ), (self.errors['not_writtable_nested'].format(
            method_name=self.method_name,
            module=self.serializer.__class__.__module__,
            class_name=self.serializer.__class__.__name__
        ))
        assert not any(
            len(field.source_attrs) > 1 and
            (field.source_attrs[0] in self.validated_data) and
            (field.source_attrs[0] in self.model_field_info.relations) and
            isinstance(self.validated_data[field.source_attrs[0]], (list, dict))
            for field in await self.serializer._writable_fields
        ), (self.errors['not_writtable_dotted_source'].format(
            method_name=self.method_name, 
            module=self.serializer.__class__.__module__,
            class_name=self.serializer.__class__.__name__
        ))
