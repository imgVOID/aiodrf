from contextlib import suppress
from django.utils.translation import gettext_lazy as _
from django.contrib.sites.shortcuts import get_current_site
from rest_framework.settings import api_settings
from rest_framework.response import Response
from rest_framework.utils.urls import remove_query_param, replace_query_param
from asgiref.sync import sync_to_async


remove_query_param = sync_to_async(remove_query_param)
replace_query_param = sync_to_async(replace_query_param)
get_current_site = sync_to_async(get_current_site)


class LimitOffsetAsyncPagination:
    default_limit = api_settings.PAGE_SIZE if api_settings.PAGE_SIZE else 100
    limit_query_param = 'page[limit]'
    limit_query_description = _('Number of results to return per page.')
    offset_query_param = 'page[offset]'
    offset_query_description = _('The initial index from which to return the results.')
    max_limit = None
    current_site = None

    @staticmethod
    async def encode_url_parameters(url):
        return url.replace('%5B', '[').replace('%5D', ']')
    
    @staticmethod
    async def positive_int(integer_string, strict=False, cutoff=None):
        ret = int(integer_string)
        if ret < 0 or (ret == 0 and strict):
            raise ValueError()
        if cutoff:
            return min(ret, cutoff)
        return ret
    
    async def get_absolute_uri(self):
        return await sync_to_async(self.request.build_absolute_uri)()
    
    async def paginate_queryset(self, queryset, request):
        self.request = request
        self.limit = await self.get_limit(request)
        if self.limit is None:
            return None
        self.count = await self.get_count(queryset)
        self.offset = await self.get_offset(request)
        if self.count == 0 or self.offset > self.count:
            return await sync_to_async(queryset.model.objects.none)()
        queryset = queryset[self.offset:self.offset + self.limit]
        return queryset

    async def get_paginated_response(self, data):
        links = {
            'self': await self.encode_url_parameters(
                await sync_to_async(self.request.build_absolute_uri)()
            )
        }
        next = await self.get_next_link()
        prev = await self.get_previous_link()
        last = await self.get_last_link()
        if next:
            links['next'] = next
        if prev:
            links['prev'] = prev
        if last != links['self']:
            links['last'] = last
        try:
            return Response({'links': links, **data})
        except TypeError:
            raise TypeError('Serializer data must be a valid dictionary.')

    async def get_paginated_response_schema(self, schema=None):
        schema = schema if schema else {
            'data': {
                'type': 'list',
                'nullable': False,
                'format': 'list_objects_jsonapi',
                'example': [{
                    'type': 'account', 'id': 1, 'attributes': {}, 
                    'relationships': {'profile': {'type': 'profile', 'id': 1}}
                }]
            },
            'included': {
                'type': 'list',
                'nullable': False,
                'format': 'list_objects_jsonapi',
                'example': [{'type': 'profile', 'id': 1, 'attributes': {}}]
            }
        }
        return {
            'links': {
                'self': {
                    'type': 'string',
                    'nullable': False,
                    'format': 'uri',
                    'example': f'http://api.example.org/accounts/?{self.offset_query_param}=200&{self.limit_query_param}=100',
                },
                'prev': {
                    'type': 'string',
                    'nullable': True,
                    'format': 'uri',
                    'example': f'http://api.example.org/accounts/?{self.offset_query_param}=100&{self.limit_query_param}=100',
                },
                'next': {
                    'type': 'string',
                    'nullable': True,
                    'format': 'uri',
                    'example': f'http://api.example.org/accounts/?{self.offset_query_param}=300&{self.limit_query_param}=100',
                },
                'last': {
                    'type': 'string',
                    'nullable': False,
                    'format': 'uri',
                    'example': f'http://api.example.org/accounts/?{self.offset_query_param}=400&{self.limit_query_param}=100',
                }
            }, **schema
        }
    
    async def get_next_link(self):
        if self.offset + self.limit >= self.count:
            return None
        else:
            url = await replace_query_param(
                await self.get_absolute_uri(), 
                self.offset_query_param, 
                self.offset + self.limit
            )
        if self.limit == self.default_limit:
            url = await remove_query_param(url, self.limit_query_param)
        else:
            url = await replace_query_param(url, self.limit_query_param, self.limit)
        return await self.encode_url_parameters(url)

    async def get_previous_link(self):
        if self.offset <= 0:
            return None
        elif self.offset - self.limit <= 0:
            url = await remove_query_param(
                await self.get_absolute_uri(), 
                self.offset_query_param
            )
        else:
            url = await replace_query_param(
                await self.get_absolute_uri(), 
                self.offset_query_param, 
                self.offset - self.limit
            )
        if self.limit == self.default_limit:
            url = await remove_query_param(url, self.limit_query_param)
        else:
            url = await replace_query_param(url, self.limit_query_param, self.limit)
        return await self.encode_url_parameters(url)
    
    async def get_last_link(self):
        url = await replace_query_param(
            await self.get_absolute_uri(), 
            self.offset_query_param, 
            self.count // self.limit * self.limit
        )
        if self.limit == self.default_limit:
            url = await remove_query_param(url, self.limit_query_param)
        else:
            url = await replace_query_param(url, self.limit_query_param, self.limit)
        return await self.encode_url_parameters(url)

    async def get_limit(self, request):
        if self.limit_query_param:
            with suppress(KeyError, ValueError):
                return await self.positive_int(
                    request.query_params[self.limit_query_param],
                    strict=True,
                    cutoff=self.max_limit
                )
        return self.default_limit

    async def get_offset(self, request):
        try:
            return await self.positive_int(
                request.query_params[self.offset_query_param],
            )
        except (KeyError, ValueError):
            return 0
    
    async def get_count(self, queryset):
        try:
            return await queryset.acount()
        except (AttributeError, TypeError):
            return len(queryset)
