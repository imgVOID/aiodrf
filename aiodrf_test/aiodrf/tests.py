from sys import platform
from asyncio import WindowsSelectorEventLoopPolicy, set_event_loop_policy
from asgiref.sync import sync_to_async
from django.test import TestCase, TransactionTestCase

from .models import Test, TestIncluded
from .serializers import ModelSerializerAsync


class TestModelSerializer(TestCase):
    fixtures = ['test_model_fixture.json']
    _main_model, _relation_model = Test, TestIncluded
    _main_query = _main_model.objects.all()
    _relation_query = _relation_model.objects.all()
    _foreign_key_name = 'foreign_key'
    _many_to_many_name = 'many_to_many'
    _many_to_many_relation_name = 'many_to_many_included'
    _text_name, _test_str = 'text', 'The new object text'
    
    @classmethod
    def setUp(cls):
        if platform == 'win32':
            set_event_loop_policy(WindowsSelectorEventLoopPolicy())
    
    @classmethod
    async def _get_serializer(cls):
        
        class Serializer(ModelSerializerAsync):
            class Meta:
                model, fields = cls._main_model, '__all__'
        
        return Serializer
    
    @classmethod
    async def _get_serializer_relation(cls):
        
        class SerializerRelation(ModelSerializerAsync):
            class Meta:
                model, fields = cls._relation_model, '__all__'
        
        return SerializerRelation
    
    async def test_nested_relation(self):
        serializer = await self._get_serializer_relation()
        
        class Serializer(ModelSerializerAsync):
            foreign_key = serializer()
            
            class Meta:
                model, fields = self._main_model, '__all__'
        
        obj = await self._main_query.afirst()
        obj_rel = await self._relation_query.afirst()
        setattr(obj, self._foreign_key_name, obj_rel)
        await obj.asave()
        serializer = Serializer(obj)
        data = await serializer.data
        rel_obj = await sync_to_async(getattr)(obj, self._foreign_key_name)
        rel_keys = data[self._foreign_key_name]
        del rel_keys[self._many_to_many_relation_name]
        [self.assertEqual(await sync_to_async(getattr)(rel_obj, key), rel_keys[key]) 
         for key in rel_keys.keys()]

    async def test_create(self):
        obj = await self._main_query.afirst()
        serializer = await self._get_serializer()
        data = await serializer(obj).data
        data[self._text_name] = self._test_str 
        data[self._foreign_key_name] = await self._relation_query.afirst()
        serializer, id = serializer(data=data), data.pop('id')
        await serializer.is_valid()
        obj = await serializer.create(await serializer.data)
        self.assertGreater(obj.id, id)
        self.assertIsInstance(
            obj.foreign_key, self._main_model.foreign_key.field.related_model
        )
        self.assertIsInstance(await self._main_model.objects.aget(text=data['text']), self._main_model)
        del data[self._foreign_key_name], data[self._many_to_many_name]
        [self.assertEqual(getattr(obj, key), data[key]) for key in data.keys()]
        
    async def test_create_list(self):
        objs = [obj async for obj in self._main_query.all()]
        serializer = await self._get_serializer()
        serializer = serializer(objs, many=True)
        data = await serializer.data
        for obj in data:
            obj[self._text_name] = self._test_str
            obj[self._foreign_key_name] = await self._relation_query.afirst()
            del obj['id']
        objs = await serializer.create(data)
        [[self.assertEqual(getattr(objs[i], k), data[i][k]) 
          for k in dict(data[i]).keys()] for i in range(len(objs))]
    
    async def test_update(self):
        obj = await self._main_query.afirst()
        serializer = await self._get_serializer()
        data = await serializer(obj).data
        self.assertEqual(data[self._foreign_key_name], None)
        data[self._foreign_key_name] = await self._relation_query.afirst()
        data[self._text_name] = self._test_str
        serializer = serializer(data=data)
        await serializer.is_valid()
        await serializer.update(obj, data)
        try:
            obj_updated = await self._main_query.aget(text=data[self._text_name])
        except obj.__class__.DoesNotExist:
            raise AssertionError('Not saved.')
        self.assertEqual(str(obj), data[self._text_name])
        self.assertEqual(str(obj_updated), data[self._text_name])
        self.assertIsInstance(await sync_to_async(getattr)(obj, self._foreign_key_name), self._relation_model)
        self.assertIsInstance(await sync_to_async(getattr)(obj_updated, self._foreign_key_name), self._relation_model)
        del data[self._foreign_key_name], data[self._many_to_many_name]
        [self.assertEqual(await sync_to_async(getattr)(obj, key), data[key]) for key in data.keys()]
        [self.assertEqual(await sync_to_async(getattr)(obj_updated, key), data[key]) for key in data.keys()]


class TestModelSerializerAsyncDbCon(TransactionTestCase):
    fixtures = ['test_model_fixture.json']
    _main_model, _relation_model = Test, TestIncluded
    _main_query = _main_model.objects.all()
    _relation_query = _relation_model.objects.all()
    _foreign_key_name = 'foreign_key'
    _many_to_many_name = 'many_to_many'
    _text_name, _test_str = 'text', 'The new object text'
    
    @classmethod
    async def _get_serializer(cls):
        return await TestModelSerializer._get_serializer()
    
    # TODO: test many to many
    async def test_acreate(self):
        obj = await self._main_query.afirst()
        serializer = await self._get_serializer()
        data = dict(await serializer(obj).data)
        data[self._text_name] = self._test_str
        data[self._many_to_many_name] = [1, 2]
        id = data.pop('id')
        obj = await serializer(data=data).acreate(data)
        self.assertGreater(obj.id, id)
        [self.assertEqual(getattr(obj, key), data[key]) for key in data.keys()]
        self.assertIsInstance(await self._main_model.objects.aget(id=obj.id), 
                              self._main_model)

    async def test_acreate_list(self):
        # Doesn't create a many to many relationship.
        objs = [obj async for obj in self._main_query.all()]
        serializer = await self._get_serializer()
        serializer = serializer(objs, many=True)
        data = await serializer.data
        objs = await serializer.acreate(data)
        for i in range(len(data)):
            self.assertGreater(objs[i].id, data[i]['id'])
            del data[i]['id']
        [[self.assertEqual(getattr(objs[i], k), data[i][k]) 
          for k in dict(data[i]).keys()] for i in range(len(objs))]

    # TODO: test many to many
    async def test_aupdate(self):
        foreign_key_name = self._foreign_key_name
        obj = await self._main_query.afirst()
        serializer = await self._get_serializer()
        data = dict(await serializer(obj).data)
        self.assertNotEqual(data[self._text_name], self._test_str)
        self.assertNotEqual(obj.text, self._test_str)
        self.assertIsNone(
            data[foreign_key_name], 
            await sync_to_async(getattr)(obj, foreign_key_name)
        )
        data[self._text_name], data[foreign_key_name] = self._test_str, None
        obj = await serializer(data=data).aupdate(data)
        obj_from_query = await self._main_model.objects.aget(id=data['id'])
        [self.assertEqual(
            await sync_to_async(getattr)(obj, key), 
            await sync_to_async(getattr)(obj_from_query, key)
        ) for key in data.keys()]
        [self.assertEqual(await sync_to_async(getattr)(obj, key), data[key]) 
         for key in data.keys()]
        self.assertIsInstance(
            await self._main_model.objects.aget(id=data['id']), 
            self._main_model
        )
        obj = await serializer(data=data).aupdate({'id': 1, foreign_key_name: 1})
        obj_from_query = await self._main_model.objects.aget(id=data['id'])
        obj_rel = await sync_to_async(getattr)(obj_from_query, self._foreign_key_name)
        self.assertIsInstance(obj_rel, self._relation_model)
        obj = await serializer(data=data).aupdate({'id': 1, foreign_key_name: 2})
        obj_from_query = await self._main_model.objects.aget(id=data['id'])
        self.assertIsInstance(obj_rel, self._relation_model)


class TestPagination(TestCase):
    @classmethod
    def setUp(cls):
        if platform == 'win32':
            set_event_loop_policy(WindowsSelectorEventLoopPolicy())

    async def test_pagination(self):
        pass