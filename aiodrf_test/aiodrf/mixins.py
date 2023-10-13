from asgiref.sync import sync_to_async
from django.db.models.query import prefetch_related_objects

from rest_framework import status
from rest_framework.response import Response
from rest_framework.settings import api_settings
from rest_framework.mixins import (CreateModelMixin, ListModelMixin, RetrieveModelMixin, 
                                   UpdateModelMixin, DestroyModelMixin)


class CreateModelAsyncMixin(CreateModelMixin):
    async def create(self, request, *args, **kwargs):
        serializer = await self.get_serializer(data=request.data)
        await serializer.is_valid(raise_exception=True)
        await self.perform_create(serializer)
        headers = await self.get_success_headers(await serializer.data)
        return Response(await serializer.data, status=status.HTTP_201_CREATED, headers=headers)

    async def perform_create(self, serializer):
        await serializer.asave()

    async def get_success_headers(self, data):
        try:
            return {'Location': str(data[api_settings.URL_FIELD_NAME])}
        except (TypeError, KeyError):
            return {}


class ListModelAsyncMixin(ListModelMixin):
    async def list(self, request, *args, **kwargs):
        queryset = await self.filter_queryset(await self.get_queryset())

        page = await self.paginate_queryset(queryset)
        if page is not None:
            serializer = await self.get_serializer(page, many=True)
            return await self.get_paginated_response(await serializer.data)

        serializer = await self.get_serializer(queryset, many=True)
        return Response(await serializer.data)


class RetrieveModelAsyncMixin(RetrieveModelMixin):
    async def retrieve(self, request, *args, **kwargs):
        instance = await self.get_object()
        serializer = await self.get_serializer(instance)
        return Response(await serializer.data)


class UpdateModelAsyncMixin(UpdateModelMixin):
    async def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = await self.get_object()
        serializer = await self.get_serializer(instance, data=request.data, partial=partial)
        await serializer.is_valid(raise_exception=True)
        await self.perform_update(serializer)

        queryset = await self.filter_queryset(await self.get_queryset())
        if queryset._prefetch_related_lookups:
            instance._prefetched_objects_cache = {}
            await sync_to_async(prefetch_related_objects)([instance], *queryset._prefetch_related_lookups)

        return Response(await serializer.data)

    async def perform_update(self, serializer):
        await serializer.asave()

    async def partial_update(self, request, *args, **kwargs):
        kwargs['partial'] = True
        return await self.aupdate(request, *args, **kwargs)


class DestroyModelAsyncMixin(DestroyModelMixin):
    async def destroy(self, request, *args, **kwargs):
        instance = await self.get_object()
        await self.perform_destroy(instance)
        return Response(status=status.HTTP_204_NO_CONTENT)

    async def perform_destroy(self, instance):
        await instance.adelete()
