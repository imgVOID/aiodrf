import asyncio
from copy import deepcopy
from traceback import format_exc
from collections.abc import Mapping
from async_property import async_property, async_cached_property
from asgiref.sync import sync_to_async
from psycopg import AsyncConnection, OperationalError
from django.db import connection
from django.core.exceptions import ValidationError as DjangoValidationError
from django.utils.functional import cached_property
from rest_framework.utils import html, model_meta
from rest_framework.serializers import (
    ListSerializer, ModelSerializer, Serializer, SerializerMetaclass,
    raise_errors_on_nested_writes, as_serializer_error
)
from rest_framework.utils.serializer_helpers import (
    BindingDict, BoundField, JSONBoundField, 
    NestedBoundField, ReturnDict, ReturnList
)
from rest_framework.exceptions import ErrorDetail, ValidationError
from rest_framework.fields import JSONField, SkipField, empty, get_error_detail 
from rest_framework.settings import api_settings
from rest_framework.relations import PKOnlyObject

raise_errors_on_nested_writes = sync_to_async(raise_errors_on_nested_writes)


class SerializerAsync(Serializer, metaclass=SerializerMetaclass):
    async def set_value(self, dictionary, keys, value):
        if not keys:
            dictionary.update(value)
            return

        for key in keys[:-1]:
            if key not in dictionary:
                dictionary[key] = {}
            dictionary = dictionary[key]

        dictionary[keys[-1]] = value

    @async_cached_property
    async def fields(self):
        fields = BindingDict(self)
        get_fields = await self.get_fields()
        for key, value in get_fields.items():
            fields[key] = value
        return fields

    @property
    async def _writable_fields(self):
        fields = await self.fields
        for field in fields.values():
            if not field.read_only:
                yield field

    @property
    async def _readable_fields(self):
        fields = await self.fields
        for field in fields.values():
            if not field.write_only:
                yield field

    async def get_fields(self):
        return await sync_to_async(deepcopy)(self._declared_fields)

    async def get_initial(self):
        fields = await self.fields
        if hasattr(self, 'initial_data'):
            if not isinstance(self.initial_data, Mapping):
                return {}

            return {
                field_name: await sync_to_async(field.get_value)(self.initial_data)
                for field_name, field in self.fields.items()
                if (await sync_to_async(field.get_value)(self.initial_data) is not empty) and
                not field.read_only
            }

        return {
            field.field_name: await field.get_initial()
            for field in fields.values()
            if not field.read_only
        }

    async def get_value(self, dictionary):
        if await sync_to_async(html.is_html_input)(dictionary):
            return await sync_to_async(
                html.parse_html_dict
            )(dictionary, prefix=self.field_name) or empty
        return dictionary.get(self.field_name, empty)

    async def run_validation(self, data=empty):
        (is_empty_value, data) = await self.validate_empty_values(data)
        if is_empty_value:
            return data

        value = await self.to_internal_value(data)
        try:
            await self.run_validators(value)
            value = await self.validate(value)
            assert value is not None, '.validate() should return the validated data'
        except (ValidationError, DjangoValidationError) as exc:
            raise ValidationError(detail=await sync_to_async(as_serializer_error)(exc))

        return value

    async def _read_only_defaults(self):
        fields = await self.fields
        fields = [
            field for field in fields.values()
            if (field.read_only) and (field.default != empty) and (field.source != '*') and ('.' not in field.source)
        ]

        defaults = {}
        for field in fields:
            try:
                default = await field.get_default()
            except SkipField:
                default = await sync_to_async(field.get_default)()
            defaults[field.source] = default

        return defaults

    async def run_validators(self, value):
        if isinstance(value, dict):
            to_validate = await self._read_only_defaults()
            to_validate.update(value)
        else:
            to_validate = value
            await super().run_validators(to_validate)

    async def to_internal_value(self, data):
        if not isinstance(data, Mapping):
            message = self.error_messages['invalid'].format(
                datatype=type(data).__name__
            )
            raise ValidationError({
                api_settings.NON_FIELD_ERRORS_KEY: [message]
            }, code='invalid')

        ret = {}
        errors = {}
        fields = await self._writable_fields

        async for field in fields:
            validate_method = getattr(self, 'validate_' + field.field_name, None)
            primitive_value = await field.get_value(data)
            try:
                validated_value = await sync_to_async(field.run_validation)(primitive_value)
                if validate_method is not None:
                    validated_value = validate_method(validated_value)
            except ValidationError as exc:
                errors[field.field_name] = exc.detail
            except DjangoValidationError as exc:
                errors[field.field_name] = await sync_to_async(get_error_detail)(exc)
            except SkipField:
                pass
            else:
                await self.set_value(ret, field.source_attrs, validated_value)

        if errors:
            raise ValidationError(errors)

        return ret

    async def to_representation(self, instance):
        ret = {}
        async for field in self._readable_fields:
            try:
                attribute = await sync_to_async(field.get_attribute)(instance)
            except SkipField:
                continue
            check_for_none = attribute.pk if isinstance(attribute, PKOnlyObject) else attribute
            if check_for_none is None:
                ret[field.field_name] = None
                continue
            try:
                ret_field = await sync_to_async(field.to_representation)(attribute)
            except TypeError:
                ret_field = await field.to_representation(attribute)
            try:
                ret_field = [await obj for obj in ret_field]
            except TypeError:
                pass
            finally:
                ret[field.field_name] = ret_field
        return ret

    async def __aiter__(self):
        try:
            fields = await self.fields
        except TypeError:
            fields = self.fields
        for field in fields.values():
            yield await self[field.field_name]

    async def __getitem__(self, key):
        try:
            fields = await self.fields
        except TypeError:
            fields = self.fields
        field = fields.get(key)
        value = await self.data
        value = value.get(key)
        error = None if not hasattr(self, '_errors') else await self.errors
        error = error.get(key) if error is not None else None
        if isinstance(field, Serializer):
            return NestedBoundField(field, value, error)
        if isinstance(field, JSONField):
            return JSONBoundField(field, value, error)
        return BoundField(field, value, error)

    @async_property
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
            if self.instance is not None and not getattr(self, '_errors', None):
                try:
                    self._data = await sync_to_async(self.to_representation)(self.instance)
                except TypeError:
                    self._data = await self.to_representation(self.instance)
            elif hasattr(self, '_validated_data') and not getattr(self, '_errors', None):
                self._data = await self.to_representation(self.validated_data)
            else:
                try:
                    self._data = await self.get_initial()
                except TypeError:
                    self._data = await sync_to_async(self.get_initial)()
        return ReturnDict(self._data, serializer=self)

    @async_property
    async def errors(self):
        return await sync_to_async(super().errors.fget)(self)
    
    @async_property
    async def validated_data(self):
        if not hasattr(self, '_validated_data'):
            msg = 'You must call `.is_valid()` before accessing `.validated_data`.'
            raise AssertionError(msg)
        return self._validated_data


class ListSerializerAsync(ListSerializer):
    
    async def __aiter__(self):
        try:
            fields = await self.child.fields
        except TypeError:
            fields = self.child.fields
        for field in fields.values():
            yield await self[field.field_name]

    async def __getitem__(self, key):
        copy = type(self.child)(self.child.instance, many=True)
        try:
            fields = await self.child.fields
        except TypeError:
            fields = self.child.fields
        field = fields.get(key)
        value = list(await copy.data).pop().get(key)
        error = None if not hasattr(self, '_errors') else await self.errors
        error = error.get(key) if error is not None else None
        if isinstance(field, Serializer):
            return NestedBoundField(field, value, error)
        if isinstance(field, JSONField):
            return JSONBoundField(field, value, error)
        return BoundField(field, value, error)

    @async_property
    async def data(self):
        data = await sync_to_async(ListSerializerAsync.__base__.data.fget)(self)
        try:
            data = [await obj for obj in data]
        except TypeError:
            pass
        return ReturnList(data, serializer=self)

    @async_property
    async def errors(self):
        try:
            ret = await sync_to_async(self.child.errors.fget)(self)
        except TypeError:
            ret = await self.child.errors.fget(self)
        if isinstance(ret, list) and len(ret) == 1 and getattr(ret[0], 'code', None) == 'null':
            detail = ErrorDetail('No data provided', code='null')
            ret = {api_settings.NON_FIELD_ERRORS_KEY: [detail]}
        if isinstance(ret, dict):
            return ReturnDict(ret, serializer=self)
        return ReturnList(ret, serializer=self)

    async def is_valid(self, *, raise_exception=False):
        return await sync_to_async(ListSerializerAsync.__base__.is_valid)(**locals())

    async def save(self, **kwargs):
        assert 'commit' not in kwargs, (
            "'commit' is not a valid keyword argument to the 'save()' method. "
            "If you need to access data before committing to the database then "
            "inspect 'serializer.validated_data' instead. "
            "You can also pass additional keyword arguments to 'save()' if you "
            "need to set extra attributes on the saved model instance. "
            "For example: 'serializer.save(owner=request.user)'.'"
        )

        validated_data = [
            {**attrs, **kwargs} for attrs in self.validated_data
        ]

        if self.instance is not None:
            self.instance = await self.update(self.instance, validated_data)
            assert self.instance is not None, (
                '`update()` did not return an object instance.'
            )
        else:
            self.instance = await self.create(validated_data)
            assert self.instance is not None, (
                '`create()` did not return an object instance.'
            )

        return self.instance
    
    async def create(self, validated_data):
        return [await self.child.create(attrs) for attrs in validated_data]

    async def acreate(self, validated_data):
        return await self.child.acreate(validated_data)

    async def update(self, instance, validated_data):
        raise NotImplementedError(
            "Serializers with many=True do not support multiple update by "
            "default, only multiple create. For updates it is unclear how to "
            "deal with insertions and deletions. If you need to support "
            "multiple update, use a `ListSerializer` class and override "
            "`.update()` so you can specify the behavior exactly."
        )

    async def aupdate(self, instance, validated_data):
        await self.update()


class ListSerializerAsync(ListSerializer):
    def __get_async(self, attr_name_str, is_thread_sensitive=True, is_property=False):
        attr_sync = getattr(type(self).__base__.__base__, attr_name_str)
        if is_property:
            attr_sync = attr_sync.fget
        return sync_to_async(attr_sync, thread_sensitive=is_thread_sensitive)

    @cached_property
    def fields(self):
        fields = BindingDict(self)
        fields_items = asyncio.run(self.__get_async('get_fields', False)(self)).items()
        for key, value in fields_items:
            fields[key] = value
        return fields

    @property
    def data(self):
        return asyncio.run(self.__get_async('data', True, True)(self))

    @property
    def errors(self):
        ret = asyncio.run(sync_to_async(self.child.errors.fget)(self))
        if isinstance(ret, list) and len(ret) == 1 and getattr(ret[0], 'code', None) == 'null':
            # Edge case. Provide a more descriptive error than
            # "this field may not be null", when no data is passed.
            detail = ErrorDetail('No data provided', code='null')
            ret = {api_settings.NON_FIELD_ERRORS_KEY: [detail]}
        if isinstance(ret, dict):
            return ReturnDict(ret, serializer=self)
        return ReturnList(ret, serializer=self)

    def is_valid(self, *args, raise_exception=False):
        return asyncio.run(self.__get_async('is_valid', False)(
            self, *args, raise_exception=raise_exception
        ))
    
    @async_property
    async def adata(self):
        return await self.__get_async('data', True, True)(self)
    
    @async_property
    async def avalidated_data(self):
        return await self.__get_async('validated_data', False, True)(self)

    @async_property
    async def aerrors(self):
        ret = await sync_to_async(self.child.errors.fget)(self)
        if isinstance(ret, list) and len(ret) == 1 and getattr(ret[0], 'code', None) == 'null':
            # Edge case. Provide a more descriptive error than
            # "this field may not be null", when no data is passed.
            detail = ErrorDetail('No data provided', code='null')
            ret = {api_settings.NON_FIELD_ERRORS_KEY: [detail]}
        if isinstance(ret, dict):
            return ReturnDict(ret, serializer=self)
        return ReturnList(ret, serializer=self)

    async def ais_valid(self, *args, raise_exception=False):
        return await self.__get_async('is_valid', False)(
            self, *args, raise_exception=raise_exception
    )

    def save(self, **kwargs):
        assert hasattr(self, '_errors'), (
            'You must call `.is_valid()` before calling `.save()`.'
        )

        assert not self.errors, (
            'You cannot call `.save()` on a serializer with invalid data.'
        )

        assert 'commit' not in kwargs, (
            "'commit' is not a valid keyword argument to the 'save()' method. "
            "If you need to access data before committing to the database then "
            "inspect 'serializer.validated_data' instead. "
            "You can also pass additional keyword arguments to 'save()' if you "
            "need to set extra attributes on the saved model instance. "
            "For example: 'serializer.save(owner=request.user)'.'"
        )

        assert not hasattr(self, '_data'), (
            "You cannot call `.save()` after accessing `serializer.data`."
            "If you need to access data before committing to the database then "
            "inspect 'serializer.validated_data' instead. "
        )

        validated_data = {**self.validated_data, **kwargs}

        if self.instance is not None:
            self.instance = asyncio.run(self.update(self.instance, validated_data))
            assert self.instance is not None, (
                '`update()` did not return an object instance.'
            )
        else:
            self.instance = asyncio.run(self.create(validated_data))
            assert self.instance is not None, (
                '`create()` did not return an object instance.'
            )

        return self.instance

    async def asave(self, **kwargs):
        assert hasattr(self, '_errors'), (
            'You must call `.is_valid()` before calling `.save()`.'
        )

        assert not await self.aerrors, (
            'You cannot call `.save()` on a serializer with invalid data.'
        )

        assert 'commit' not in kwargs, (
            "'commit' is not a valid keyword argument to the 'save()' method. "
            "If you need to access data before committing to the database then "
            "inspect 'serializer.validated_data' instead. "
            "You can also pass additional keyword arguments to 'save()' if you "
            "need to set extra attributes on the saved model instance. "
            "For example: 'serializer.save(owner=request.user)'.'"
        )

        assert not hasattr(self, '_data'), (
            "You cannot call `.save()` after accessing `serializer.data`."
            "If you need to access data before committing to the database then "
            "inspect 'serializer.validated_data' instead. "
        )

        validated_data = {**await self.avalidated_data, **kwargs}

        if self.instance is not None:
            self.instance = await self.update(self.instance, validated_data)
            assert self.instance is not None, (
                '`update()` did not return an object instance.'
            )
        else:
            self.instance = await self.create(validated_data)
            assert self.instance is not None, (
                '`create()` did not return an object instance.'
            )

        return self.instance
    
    async def create(self, validated_data):
        return [await self.child.create(attrs) for attrs in validated_data]

    async def acreate(self, validated_data):
        return await self.child.acreate(validated_data)

    async def update(self, instance, validated_data):
        raise NotImplementedError(
            "Serializers with many=True do not support multiple update by "
            "default, only multiple create. For updates it is unclear how to "
            "deal with insertions and deletions. If you need to support "
            "multiple update, use a `ListSerializer` class and override "
            "`.update()` so you can specify the behavior exactly."
        )

    async def aupdate(self, instance, validated_data):
        await self.update()


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

    def __get_async(self, attr_name_str, is_thread_sensitive=True, is_property=False):
        attr_sync = getattr(type(self).__base__.__base__, attr_name_str)
        if is_property:
            attr_sync = attr_sync.fget
        return sync_to_async(attr_sync, thread_sensitive=is_thread_sensitive)

    @cached_property
    def fields(self):
        fields = BindingDict(self)
        fields_items = asyncio.run(self.__get_async('get_fields', False)(self)).items()
        for key, value in fields_items:
            fields[key] = value
        return fields

    @property
    def validated_data(self):
        return asyncio.run(self.__get_async('validated_data', False, True)(self))

    @property
    def data(self):
        return asyncio.run(self.__get_async('data', True, True)(self))

    @property
    def errors(self):
        return asyncio.run(self.__get_async('errors', True, True)(self))

    def is_valid(self, *args, raise_exception=False):
        return asyncio.run(self.__get_async('is_valid', False)(
            self, *args, raise_exception=raise_exception
        ))

    def save(self, **kwargs):
        assert hasattr(self, '_errors'), (
            'You must call `.is_valid()` before calling `.save()`.'
        )

        assert not self.errors, (
            'You cannot call `.save()` on a serializer with invalid data.'
        )

        assert 'commit' not in kwargs, (
            "'commit' is not a valid keyword argument to the 'save()' method. "
            "If you need to access data before committing to the database then "
            "inspect 'serializer.validated_data' instead. "
            "You can also pass additional keyword arguments to 'save()' if you "
            "need to set extra attributes on the saved model instance. "
            "For example: 'serializer.save(owner=request.user)'.'"
        )

        assert not hasattr(self, '_data'), (
            "You cannot call `.save()` after accessing `serializer.data`."
            "If you need to access data before committing to the database then "
            "inspect 'serializer.validated_data' instead. "
        )

        validated_data = {**self.validated_data, **kwargs}

        if self.instance is not None:
            self.instance = asyncio.run(self.update(self.instance, validated_data))
            assert self.instance is not None, (
                '`update()` did not return an object instance.'
            )
        else:
            self.instance = asyncio.run(self.create(validated_data))
            assert self.instance is not None, (
                '`create()` did not return an object instance.'
            )

        return self.instance
    
    @async_property
    async def adata(self):
        return await self.__get_async('data', True, True)(self)
    
    @async_property
    async def avalidated_data(self):
        return await self.__get_async('validated_data', False, True)(self)

    @async_property
    async def aerrors(self):
        return await self.__get_async('errors', True, True)(self)

    async def ais_valid(self, *args, raise_exception=False):
        return await self.__get_async('is_valid', False)(
            self, *args, raise_exception=raise_exception
    )

    # Add async db connection possibilities
    async def asave(self, **kwargs):
        assert hasattr(self, '_errors'), (
            'You must call `.is_valid()` before calling `.save()`.'
        )

        assert not await self.aerrors, (
            'You cannot call `.save()` on a serializer with invalid data.'
        )

        assert 'commit' not in kwargs, (
            "'commit' is not a valid keyword argument to the 'save()' method. "
            "If you need to access data before committing to the database then "
            "inspect 'serializer.validated_data' instead. "
            "You can also pass additional keyword arguments to 'save()' if you "
            "need to set extra attributes on the saved model instance. "
            "For example: 'serializer.save(owner=request.user)'.'"
        )

        assert not hasattr(self, '_data'), (
            "You cannot call `.save()` after accessing `serializer.data`."
            "If you need to access data before committing to the database then "
            "inspect 'serializer.validated_data' instead. "
        )

        validated_data = {**await self.avalidated_data, **kwargs}

        if self.instance is not None:
            self.instance = await self.update(self.instance, validated_data)
            assert self.instance is not None, (
                '`update()` did not return an object instance.'
            )
        else:
            self.instance = await self.create(validated_data)
            assert self.instance is not None, (
                '`create()` did not return an object instance.'
            )

        return self.instance

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
                field = getattr(instance, field_name)
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
                await sync_to_async(setattr)(instance, attr, value)

        await instance.asave()
        
        for attr, value in m2m_fields:
            field = getattr(instance, attr)
            await field.aset(value)

        return instance

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
