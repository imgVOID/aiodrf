from traceback import format_exc
from async_property import async_property
from asgiref.sync import sync_to_async
from psycopg import AsyncConnection, OperationalError
from django.db import connection
from rest_framework.utils import model_meta
from rest_framework.serializers import (
    ListSerializer, ModelSerializer, 
    raise_errors_on_nested_writes
)

setattr, getattr = sync_to_async(setattr), sync_to_async(getattr)
raise_errors_on_nested_writes = sync_to_async(raise_errors_on_nested_writes)


class ListSerializerAsync(ListSerializer):
    
    @async_property
    async def data(self):
        return await sync_to_async(self.__class__.__mro__[1].data.fget)(self)
    
    @async_property
    async def errors(self):
        return await sync_to_async(self.__class__.__mro__[1].errors.fget)(self)

    async def is_valid(self, *, raise_exception=False):
        return await sync_to_async(self.__class__.__mro__[1].is_valid)(**locals())
    
    async def create(self, validated_data):
        return [await self.child.create(attrs) for attrs in validated_data]

    async def acreate(self, validated_data):
        return await self.child.acreate(validated_data)

    async def aupdate(self, instance, validated_data):
        return await self.update(instance, validated_data)


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
    
    @async_property
    async def data(self):
        return await sync_to_async(self.__class__.__mro__[-3].data.fget)(self)

    @async_property
    async def errors(self):
        return await sync_to_async(self.__class__.__mro__[-3].errors.fget)(self)

    @async_property
    async def validated_data(self):
        return await sync_to_async(self.__class__.__mro__[-3].validated_data.fget)(self)
    
    async def is_valid(self, *args, raise_exception=False):
        await sync_to_async(self.__class__.__mro__[-3].is_valid)(
            self, *args, raise_exception=raise_exception
        )

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
