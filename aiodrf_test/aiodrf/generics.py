from asgiref.sync import sync_to_async
from django.core.exceptions import ValidationError
from django.db.models.query import QuerySet
from django.http import Http404
from django.shortcuts import get_object_or_404 as _get_object_or_404

from rest_framework.settings import api_settings

from . import views, mixins


async def get_object_or_404(queryset, *filter_args, **filter_kwargs):
    try:
        return await sync_to_async(_get_object_or_404)(queryset, *filter_args, **filter_kwargs)
    except (TypeError, ValueError, ValidationError):
        raise Http404


class GenericAPIView(views.APIView):
    queryset, serializer_class = None, None
    lookup_field, lookup_url_kwarg = 'pk', None
    filter_backends = api_settings.DEFAULT_FILTER_BACKENDS
    pagination_class = api_settings.DEFAULT_PAGINATION_CLASS

    def __class_getitem__(cls, *args, **kwargs):
        return cls

    async def get_queryset(self):
        assert self.queryset is not None, (
            "'%s' should either include a `queryset` attribute, "
            "or override the `get_queryset()` method."
            % self.__class__.__name__
        )

        queryset = self.queryset
        if isinstance(queryset, QuerySet):
            queryset = queryset.all()
        return queryset

    async def get_object(self):
        queryset = await self.filter_queryset(await self.get_queryset())

        lookup_url_kwarg = self.lookup_url_kwarg or self.lookup_field

        assert lookup_url_kwarg in self.kwargs, (
            'Expected view %s to be called with a URL keyword argument '
            'named "%s". Fix your URL conf, or set the `.lookup_field` '
            'attribute on the view correctly.' %
            (self.__class__.__name__, lookup_url_kwarg)
        )

        filter_kwargs = {self.lookup_field: self.kwargs[lookup_url_kwarg]}
        obj = await sync_to_async(get_object_or_404)(queryset, **filter_kwargs)

        await self.check_object_permissions(self.request, obj)

        return obj

    async def get_serializer(self, *args, **kwargs):
        serializer_class = await self.get_serializer_class()
        kwargs.setdefault('context', await self.get_serializer_context())
        return serializer_class(*args, **kwargs)

    async def get_serializer_class(self):
        assert self.serializer_class is not None, (
            "'%s' should either include a `serializer_class` attribute, "
            "or override the `get_serializer_class()` method."
            % self.__class__.__name__
        )

        return self.serializer_class

    async def get_serializer_context(self):
        return {
            'request': self.request,
            'format': self.format_kwarg,
            'view': self
        }

    async def filter_queryset(self, queryset):
        for backend in list(self.filter_backends):
            queryset = backend().filter_queryset(self.request, queryset, self)
        return queryset

    @property
    async def paginator(self):
        if not hasattr(self, '_paginator'):
            if self.pagination_class is None:
                self._paginator = None
            else:
                self._paginator = await self.pagination_class()
        return self._paginator

    async def paginate_queryset(self, queryset):
        if self.paginator is None:
            return None
        return await self.paginator.paginate_queryset(queryset, self.request, view=self)

    async def get_paginated_response(self, data):
        assert self.paginator is not None
        return await self.paginator.get_paginated_response(data)


class CreateAPIView(mixins.CreateModelAsyncMixin,
                    GenericAPIView):
    async def post(self, request, *args, **kwargs):
        return await self.create(request, *args, **kwargs)


class ListAPIView(mixins.ListModelAsyncMixin,
                  GenericAPIView):
    async def get(self, request, *args, **kwargs):
        return await self.list(request, *args, **kwargs)


class RetrieveAPIView(mixins.RetrieveModelAsyncMixin,
                      GenericAPIView):
    async def get(self, request, *args, **kwargs):
        return await self.retrieve(request, *args, **kwargs)


class DestroyAPIView(mixins.DestroyModelAsyncMixin,
                     GenericAPIView):
    async def delete(self, request, *args, **kwargs):
        return await self.destroy(request, *args, **kwargs)


class UpdateAPIView(mixins.UpdateModelAsyncMixin,
                    GenericAPIView):
    async def put(self, request, *args, **kwargs):
        return await self.update(request, *args, **kwargs)

    async def patch(self, request, *args, **kwargs):
        return await self.partial_update(request, *args, **kwargs)


class ListCreateAPIView(mixins.ListModelAsyncMixin,
                        mixins.CreateModelAsyncMixin,
                        GenericAPIView):
    async def get(self, request, *args, **kwargs):
        return await self.list(request, *args, **kwargs)

    async def post(self, request, *args, **kwargs):
        return await self.create(request, *args, **kwargs)


class RetrieveUpdateAPIView(mixins.RetrieveModelAsyncMixin,
                            mixins.UpdateModelAsyncMixin,
                            GenericAPIView):
    async def get(self, request, *args, **kwargs):
        return await self.retrieve(request, *args, **kwargs)

    async def put(self, request, *args, **kwargs):
        return await self.update(request, *args, **kwargs)

    async def patch(self, request, *args, **kwargs):
        return await self.partial_update(request, *args, **kwargs)


class RetrieveDestroyAPIView(mixins.RetrieveModelAsyncMixin,
                             mixins.DestroyModelAsyncMixin,
                             GenericAPIView):
    async def get(self, request, *args, **kwargs):
        return await self.retrieve(request, *args, **kwargs)

    async def delete(self, request, *args, **kwargs):
        return await self.destroy(request, *args, **kwargs)


class RetrieveUpdateDestroyAPIView(mixins.RetrieveModelAsyncMixin,
                                   mixins.UpdateModelAsyncMixin,
                                   mixins.DestroyModelAsyncMixin,
                                   GenericAPIView):
    async def get(self, request, *args, **kwargs):
        return await self.retrieve(request, *args, **kwargs)

    async def put(self, request, *args, **kwargs):
        return await self.update(request, *args, **kwargs)

    async def patch(self, request, *args, **kwargs):
        return await self.partial_update(request, *args, **kwargs)

    async def delete(self, request, *args, **kwargs):
        return await self.destroy(request, *args, **kwargs)
