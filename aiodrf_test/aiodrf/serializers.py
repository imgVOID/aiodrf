from psycopg import AsyncConnection, OperationalError
from django.db import connection
from traceback import format_exc
from asgiref.sync import sync_to_async

from rest_framework.serializers import BaseSerializer, ListSerializer, ModelSerializer
from rest_framework.utils import model_meta
from rest_framework.exceptions import ErrorDetail, ValidationError
from rest_framework.utils.serializer_helpers import (ReturnDict, ReturnList)
from rest_framework.settings import api_settings

setattr, getattr = sync_to_async(setattr), sync_to_async(getattr)


async def raise_errors_on_nested_writes(method_name, serializer, validated_data):
    try:
        ModelClass = serializer.Meta.model
    except AttributeError:
        ModelClass = serializer.child.Meta.model
    model_field_info = await sync_to_async(model_meta.get_field_info)(ModelClass)
    try:
        writable_fields = serializer._writable_fields
    except AttributeError:
        writable_fields = serializer.child._writable_fields
    assert not any(
        isinstance(field, BaseSerializer) and
        (field.source in validated_data) and
        (field.source in model_field_info.relations) and
        isinstance(validated_data[field.source], (list, dict))
        for field in writable_fields
    ), (
        'The `.{method_name}()` method does not support writable nested '
        'fields by default.\nWrite an explicit `.{method_name}()` method for '
        'serializer `{module}.{class_name}`, or set `read_only=True` on '
        'nested serializer fields.'.format(
            method_name=method_name,
            module=serializer.__class__.__module__,
            class_name=serializer.__class__.__name__
        )
    )
    assert not any(
        len(field.source_attrs) > 1 and
        (field.source_attrs[0] in validated_data) and
        (field.source_attrs[0] in model_field_info.relations) and
        isinstance(validated_data[field.source_attrs[0]], (list, dict))
        for field in writable_fields
    ), (
        'The `.{method_name}()` method does not support writable dotted-source '
        'fields by default.\nWrite an explicit `.{method_name}()` method for '
        'serializer `{module}.{class_name}`, or set `read_only=True` on '
        'dotted-source serializer fields.'.format(
            method_name=method_name,
            module=serializer.__class__.__module__,
            class_name=serializer.__class__.__name__
        )
    )


class ListSerializerAsync(ListSerializer):
    async def update(self, instance, validated_data):
        raise NotImplementedError(
            "Serializers with many=True do not support multiple update by "
            "default, only multiple create. For updates it is unclear how to "
            "deal with insertions and deletions. If you need to support "
            "multiple update, use a `ListSerializer` class and override "
            "`.update()` so you can specify the behavior exactly."
        )
    
    async def aupdate(self, instance, validated_data):
        return await self.update(instance, validated_data)
    
    async def create(self, validated_data):
        return [await self.child.create(attrs) for attrs in validated_data]
    
    async def acreate(self, validated_data):
        return await self.child.acreate(validated_data)
    
    @property
    async def data(self):
        if hasattr(self, 'initial_data') and not hasattr(self, '_validated_data'):
            msg = (
                'When a serializer is passed a `data` keyword argument you '
                'must call `.is_valid()` before attempting to access the '
                'serialized `.data` representation.\n'
                'You should either call `.is_valid()` first, '
                'or access `.initial_data` instead.'
            )
            raise AssertionError(msg)
        if not hasattr(self, '_data'):
            if self.instance is not None and not await getattr(self, '_errors', None):
                self._data = await sync_to_async(self.to_representation)(self.instance)
            elif hasattr(self, '_validated_data') and not await getattr(self, '_errors', None):
                self._data = await sync_to_async(self.to_representation)(self.validated_data)
            else:
                self._data = await sync_to_async(self.get_initial)()
        return ReturnList(self._data, serializer=self)
    
    @property
    async def errors(self):
        if not hasattr(self, '_errors'):
            msg = 'You must call `.is_valid()` before accessing `.errors`.'
            raise AssertionError(msg)
        ret = self._errors
        if isinstance(ret, list) and len(ret) == 1 and await getattr(ret[0], 'code', None) == 'null':
            detail = ErrorDetail('No data provided', code='null')
            ret = {api_settings.NON_FIELD_ERRORS_KEY: [detail]}
        if isinstance(ret, dict):
            return ReturnDict(ret, serializer=self)
        return ReturnList(ret, serializer=self)
    
    async def is_valid(self, *, raise_exception=False):
        assert hasattr(self, 'initial_data'), (
            'Cannot call `.is_valid()` as no `data=` keyword argument was '
            'passed when instantiating the serializer instance.'
        )

        if not hasattr(self, '_validated_data'):
            try:
                self._validated_data = await sync_to_async(
                    self.run_validation
                )(self.initial_data)
            except ValidationError as exc:
                self._validated_data = []
                self._errors = exc.detail
            else:
                self._errors = []

        if self._errors and raise_exception:
            raise ValidationError(await self.errors)

        return not bool(self._errors)


class ModelSerializerAsync(ModelSerializer):
    class Meta:
        list_serializer_class = ListSerializerAsync
    
    @classmethod
    def many_init(cls, *args, **kwargs):
        allow_empty = kwargs.pop('allow_empty', None)
        max_length = kwargs.pop('max_length', None)
        min_length = kwargs.pop('min_length', None)
        child_serializer = cls(*args, **kwargs)
        list_kwargs = {
            'child': child_serializer,
        }
        if allow_empty is not None:
            list_kwargs['allow_empty'] = allow_empty
        if max_length is not None:
            list_kwargs['max_length'] = max_length
        if min_length is not None:
            list_kwargs['min_length'] = min_length
        LIST_SERIALIZER_KWARGS = (
            'read_only', 'write_only', 'required', 'default', 'initial', 'source',
            'label', 'help_text', 'style', 'error_messages', 'allow_empty',
            'instance', 'data', 'partial', 'context', 'allow_null',
            'max_length', 'min_length'
        )
        list_kwargs.update({
            key: value for key, value in kwargs.items()
            if key in LIST_SERIALIZER_KWARGS
        })
        try:
            return cls.Meta.list_serializer_class(*args, **list_kwargs)
        except AttributeError:
            return ListSerializerAsync(*args, **list_kwargs)
    
    async def create(self, validated_data):
        await raise_errors_on_nested_writes('create', self, validated_data)
        ModelClass = self.Meta.model
        info = await sync_to_async(model_meta.get_field_info)(ModelClass)
        many_to_many = {}
        for field_name, relation_info in info.relations.items():
            if relation_info.to_many and (field_name in validated_data):
                many_to_many[field_name] = validated_data.pop(field_name)

        try:
            instance = await ModelClass._default_manager.acreate(**validated_data)
        except TypeError:
            tb = format_exc()
            msg = (
                'Got a `TypeError` when calling `%s.%s.create()`. '
                'This may be because you have a writable field on the '
                'serializer class that is not a valid argument to '
                '`%s.%s.create()`. You may need to make the field '
                'read-only, or override the %s.create() method to handle '
                'this correctly.\nOriginal exception was:\n %s' %
                (
                    ModelClass.__name__,
                    ModelClass._default_manager.name,
                    ModelClass.__name__,
                    ModelClass._default_manager.name,
                    self.__class__.__name__,
                    tb
                )
            )
            raise TypeError(msg)

        if many_to_many:
            for field_name, value in many_to_many.items():
                field = await getattr(instance, field_name)
                await field.aset(value)

        return instance

    async def update(self, instance, validated_data):
        await raise_errors_on_nested_writes('update', self, validated_data)
        info = await sync_to_async(model_meta.get_field_info)(instance)

        m2m_fields = []
        for attr, value in validated_data.items():
            if attr in info.relations and info.relations[attr].to_many:
                m2m_fields.append((attr, value))
            else:
                await setattr(instance, attr, value)

        await instance.asave()
        
        for attr, value in m2m_fields:
            field = await getattr(instance, attr)
            await field.aset(value)

        return instance

    @property
    async def data(self):
        if hasattr(self, 'initial_data') and not hasattr(self, '_validated_data'):
            msg = (
                'When a serializer is passed a `data` keyword argument you '
                'must call `.is_valid()` before attempting to access the '
                'serialized `.data` representation.\n'
                'You should either call `.is_valid()` first, '
                'or access `.initial_data` instead.'
            )
            raise AssertionError(msg)

        if not hasattr(self, '_data'):
            if self.instance is not None and not await getattr(self, '_errors', None):
                self._data = await sync_to_async(self.to_representation)(self.instance)
            elif hasattr(self, '_validated_data') and not await getattr(self, '_errors', None):
                self._data = await sync_to_async(self.to_representation)(self.validated_data)
            else:
                self._data = await sync_to_async(self.get_initial)()
        return self._data

    @property
    async def errors(self):
        if not hasattr(self, '_errors'):
            msg = 'You must call `.is_valid()` before accessing `.errors`.'
            raise AssertionError(msg)
        return self._errors

    @property
    async def validated_data(self):
        if not hasattr(self, '_validated_data'):
            msg = 'You must call `.is_valid()` before accessing `.validated_data`.'
            raise AssertionError(msg)
        return self._validated_data
    
    async def is_valid(self, *args, raise_exception=False):
        await sync_to_async(BaseSerializer.is_valid)(self, *args, raise_exception=False)

    # TODO: write the bulk many to many edition functionality
    async def acreate(self, validated_data):
        validated_data = [validated_data] if type(validated_data) == dict else validated_data
        await raise_errors_on_nested_writes('create', self, validated_data)
        ModelClass = self.Meta.model if hasattr(self.Meta, 'model') else self.child.Meta.model
        info = await sync_to_async(model_meta.get_field_info)(ModelClass)
        many_to_many = {}
        for data in validated_data:
            for field_name, relation_info in info.relations.items():
                rel_data = data.pop(field_name, None)
                if not relation_info.to_many:
                    data[field_name + '_id'] = rel_data
                elif rel_data is not None:
                    many_to_many[field_name] = [{
                        relation_info.related_model.__name__.lower() + '_id': x
                    } for x in rel_data]
        con_params = connection.get_connection_params()
        con_params.pop('cursor_factory')
        async with await AsyncConnection.connect(**con_params) as aconn:
            async with aconn.cursor() as cur:
                table_name = f'{ModelClass._meta.app_label}_{ModelClass.__name__.lower()}'
                keys = list(key for key in validated_data[0].keys() if key != 'id')
                values = list(list(data[key] for key in keys) for data in validated_data)
                is_many = True if len(values) > 1 else False
                execute_func = cur.executemany if is_many else cur.execute
                try:
                    await execute_func(
                        f'INSERT INTO {table_name} ({", ".join(keys)}) VALUES '
                        f'({"".join(("%s, "*len(keys)).rsplit(", ", 1))}) RETURNING *;',
                        values if is_many else values[0]
                    )
                except OperationalError as e:
                    raise OperationalError(
                        f'The {table_name} table has not been modified.'
                    ) from e
                if is_many:
                    await cur.execute(f'SELECT * FROM {table_name} ORDER BY ID DESC LIMIT {len(values)}')
                    return [ModelClass(*obj) for obj in sorted(await cur.fetchall())]
                result = ModelClass(*await cur.fetchone())
                for key, val in many_to_many.items():
                    for x in val:
                        x[ModelClass.__name__.lower() + '_id'] = result.id
                    table_name_many = f'{ModelClass._meta.app_label}_{ModelClass.__name__.lower()}_{key}'
                    if val:
                        try:
                            await cur.executemany(
                                f'INSERT INTO {table_name_many} ({", ".join(val[0].keys())}) '
                                f'VALUES ({"".join(("%s, " * len(val[0].keys())).rsplit(", ", 1))})',
                                [list(x.values()) for x in many_to_many[key]]
                            )
                        except OperationalError as e:
                            raise OperationalError(
                                f'The {table_name} table has not been modified.'
                            ) from e
                return result

    async def aupdate(self, validated_data):
        if 'id' not in validated_data:
            raise ValueError('Please specify the object id.')
        await raise_errors_on_nested_writes('update', self, validated_data)
        ModelClass = self.Meta.model
        table_name = f'{ModelClass._meta.app_label}_{ModelClass.__name__.lower()}'
        info = await sync_to_async(model_meta.get_field_info)(ModelClass)
        many_to_many = {}
        for field_name, relation_info in info.relations.items():
            rel_data = validated_data.pop(field_name, None)
            if not relation_info.to_many:
                validated_data[field_name + '_id'] = rel_data
            elif rel_data is not None:
                many_to_many[field_name] = [{
                    ModelClass.__name__.lower() + '_id': validated_data['id'],
                    relation_info.related_model.__name__.lower() + '_id': x
                } for x in rel_data]
        con_params = connection.get_connection_params()
        con_params.pop('cursor_factory', None)
        async with await AsyncConnection.connect(**con_params) as aconn:
            async with aconn.cursor() as cur:
                try:
                    for key, val in many_to_many.items():
                        table_name_many = f'{ModelClass._meta.app_label}_{ModelClass.__name__.lower()}_{key}'
                        await cur.execute(f'DELETE FROM {table_name_many};')
                        if val:
                            await cur.executemany(
                                f'INSERT INTO {table_name_many} ({", ".join(val[0].keys())}) '
                                f'VALUES ({"".join(("%s, " * len(val[0].keys())).rsplit(", ", 1))})',
                                [list(x.values()) for x in many_to_many[key]]
                            )
                    await cur.execute(
                        f'UPDATE {table_name} SET '
                        f'{" = %s, ".join(validated_data.keys())} = %s '
                        f'WHERE ID = {validated_data["id"]} RETURNING *;',
                        list(validated_data.values())
                    )
                except OperationalError as e:
                    raise OperationalError(
                        f'The {table_name} table has not been modified.'
                    ) from e
                else:
                    return ModelClass(*await cur.fetchone())
