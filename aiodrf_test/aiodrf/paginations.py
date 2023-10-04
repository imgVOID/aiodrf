import warnings
import contextlib
from rest_framework.pagination import PageNumberPagination, LimitOffsetPagination, CursorPagination
from rest_framework.utils.urls import remove_query_param, replace_query_param
from rest_framework.exceptions import NotFound
from rest_framework.response import Response
from rest_framework.compat import coreapi, coreschema
from django.template import loader
from django.utils.translation import gettext_lazy as _
from django.core.paginator import InvalidPage
from django.utils.encoding import force_str
from collections import namedtuple
from asgiref.sync import sync_to_async

remove_query_param, replace_query_param = sync_to_async(remove_query_param), sync_to_async(replace_query_param)
force_str = sync_to_async(force_str)

Cursor = namedtuple('Cursor', ['offset', 'reverse', 'position'])
PageLink = namedtuple('PageLink', ['url', 'number', 'is_active', 'is_break'])
PAGE_BREAK = PageLink(url=None, number=None, is_active=False, is_break=True)


async def _reverse_ordering(ordering_tuple):
    async def invert(x):
        return x[1:] if x.startswith('-') else '-' + x

    return tuple([await invert(item) for item in ordering_tuple])


async def _get_displayed_page_numbers(current, final):
    assert current >= 1
    assert final >= current

    if final <= 5:
        return list(range(1, final + 1))
    included = {1, current - 1, current, current + 1, final}
    
    if current <= 4:
        included.add(2)
        included.add(3)
    if current >= final - 3:
        included.add(final - 1)
        included.add(final - 2)

    included = [
        idx for idx in sorted(included)
        if 0 < idx <= final
    ]

    if current > 4:
        included.insert(1, None)
    if current < final - 3:
        included.insert(len(included) - 1, None)
    return included


async def _get_page_links(page_numbers, current, url_func):
    page_links = []
    for page_number in page_numbers:
        if page_number is None:
            page_link = PAGE_BREAK
        else:
            page_link = PageLink(
                url=await url_func(page_number),
                number=page_number,
                is_active=(page_number == current),
                is_break=False
            )
        page_links.append(page_link)
    return page_links


async def _positive_int(integer_string, strict=False, cutoff=None):
    ret = int(integer_string)
    if ret < 0 or (ret == 0 and strict):
        raise ValueError()
    if cutoff:
        return min(ret, cutoff)
    return ret


class PageNumberAsyncPagination(PageNumberPagination):
    async def paginate_queryset(self, queryset, request, view=None):
        self.request = request
        page_size = await self.get_page_size(request)
        if not page_size:
            return None

        paginator = self.django_paginator_class(queryset, page_size)
        page_number = await self.get_page_number(request, paginator)

        try:
            self.page = await sync_to_async(paginator.page)(page_number)
        except InvalidPage as exc:
            msg = self.invalid_page_message.format(
                page_number=page_number, message=str(exc)
            )
            raise NotFound(msg)

        if paginator.num_pages > 1 and self.template is not None:
            self.display_page_controls = True

        return list(self.page)

    async def get_page_number(self, request, paginator):
        page_number = request.query_params.get(self.page_query_param) or 1
        if page_number in self.last_page_strings:
            page_number = paginator.num_pages
        return page_number

    async def get_paginated_response(self, data):
        return Response({
            'count': self.page.paginator.count,
            'next': await self.get_next_link(),
            'previous': await self.get_previous_link(),
            'results': data,
        })

    async def get_paginated_response_schema(self, schema):
        return sync_to_async(super().get_paginated_response_schema)(self, schema)

    async def get_page_size(self, request):
        return await sync_to_async(super().get_page_size)(self, request)

    async def get_next_link(self):
        if not await self.page.has_next():
            return None
        url = await sync_to_async(self.request.build_absolute_uri)()
        page_number = await self.page.next_page_number()
        return await replace_query_param(url, self.page_query_param, page_number)

    async def get_previous_link(self):
        if not await self.page.has_previous():
            return None
        url = await sync_to_async(self.request.build_absolute_uri)()
        page_number = await self.page.previous_page_number()
        if page_number == 1:
            return await remove_query_param(url, self.page_query_param)
        return await replace_query_param(url, self.page_query_param, page_number)

    async def get_html_context(self):
        base_url = await sync_to_async(self.request.build_absolute_uri)()

        async def page_number_to_url(page_number):
            if page_number == 1:
                return await remove_query_param(base_url, self.page_query_param)
            else:
                return await replace_query_param(base_url, self.page_query_param, page_number)

        current = self.page.number
        final = self.page.paginator.num_pages
        page_numbers = await _get_displayed_page_numbers(current, final)
        page_links = await _get_page_links(page_numbers, current, page_number_to_url)

        return {
            'previous_url': await self.get_previous_link(),
            'next_url': await self.get_next_link(),
            'page_links': page_links
        }

    async def to_html(self):
        template = await sync_to_async(loader.get_template)(self.template)
        context = await self.get_html_context()
        return await sync_to_async(template.render)(context)

    async def get_schema_fields(self, view):
        assert coreapi is not None, 'coreapi must be installed to use `get_schema_fields()`'
        if coreapi is not None:
            warnings.warn('CoreAPI compatibility is deprecated and will be removed in DRF 3.17')
        assert coreschema is not None, 'coreschema must be installed to use `get_schema_fields()`'
        fields = [
            coreapi.Field(
                name=self.page_query_param,
                required=False,
                location='query',
                schema=coreschema.Integer(
                    title='Page',
                    description=await force_str(self.page_query_description)
                )
            )
        ]
        if await self.page_size_query_param is not None:
            fields.append(
                coreapi.Field(
                    name=self.page_size_query_param,
                    required=False,
                    location='query',
                    schema=coreschema.Integer(
                        title='Page size',
                        description=await force_str(self.page_size_query_description)
                    )
                )
            )
        return fields

    async def get_schema_operation_parameters(self, view):
        parameters = [
            {
                'name': self.page_query_param,
                'required': False,
                'in': 'query',
                'description': await force_str(self.page_query_description),
                'schema': {
                    'type': 'integer',
                },
            },
        ]
        if self.page_size_query_param is not None:
            parameters.append(
                {
                    'name': self.page_size_query_param,
                    'required': False,
                    'in': 'query',
                    'description': await force_str(self.page_size_query_description),
                    'schema': {
                        'type': 'integer',
                    },
                },
            )
        return parameters


class LimitOffsetAsyncPagination(LimitOffsetPagination):
    async def paginate_queryset(self, queryset, request, view=None):
        self.request = request
        self.limit = await self.get_limit(request)
        if self.limit is None:
            return None

        self.count = await self.get_count(queryset)
        self.offset = await self.get_offset(request)
        if self.count > self.limit and self.template is not None:
            self.display_page_controls = True

        if self.count == 0 or self.offset > self.count:
            return []
        return list(queryset[self.offset:self.offset + self.limit])

    async def get_paginated_response(self, data):
        return Response({
            'count': self.count,
            'next': await self.get_next_link(),
            'previous': await self.get_previous_link(),
            'results': data
        })

    async def get_paginated_response_schema(self, schema):
        return sync_to_async(super().get_paginated_response_schema)(self, schema)

    async def get_limit(self, request):
        if self.limit_query_param:
            with sync_to_async(contextlib.suppress)(KeyError, ValueError):
                return await _positive_int(
                    request.query_params[self.limit_query_param],
                    strict=True,
                    cutoff=self.max_limit
                )
        return self.default_limit

    async def get_offset(self, request):
        try:
            return await _positive_int(
                request.query_params[self.offset_query_param],
            )
        except (KeyError, ValueError):
            return 0

    async def get_next_link(self):
        if self.offset + self.limit >= self.count:
            return None

        url = await sync_to_async(self.request.build_absolute_uri)()
        url = await replace_query_param(url, self.limit_query_param, self.limit)

        offset = self.offset + self.limit
        return await replace_query_param(url, self.offset_query_param, offset)

    async def get_previous_link(self):
        if self.offset <= 0:
            return None

        url = await sync_to_async(self.request.build_absolute_uri)()
        url = await replace_query_param(url, self.limit_query_param, self.limit)

        if self.offset - self.limit <= 0:
            return await remove_query_param(url, self.offset_query_param)

        offset = self.offset - self.limit
        return await replace_query_param(url, self.offset_query_param, offset)

    async def get_html_context(self):
        base_url = self.request.build_absolute_uri()

        if self.limit:
            current = await _divide_with_ceil(self.offset, self.limit) + 1

            final = (
                await _divide_with_ceil(self.count - self.offset, self.limit) +
                await _divide_with_ceil(self.offset, self.limit)
            )

            final = max(final, 1)
        else:
            current = 1
            final = 1

        if current > final:
            current = final

        async def page_number_to_url(page_number):
            if page_number == 1:
                return await remove_query_param(base_url, self.offset_query_param)
            else:
                offset = self.offset + ((page_number - current) * self.limit)
                return await replace_query_param(base_url, self.offset_query_param, offset)

        page_numbers = await _get_displayed_page_numbers(current, final)
        page_links = await _get_page_links(page_numbers, current, page_number_to_url)

        return {
            'previous_url': await self.get_previous_link(),
            'next_url': await self.get_next_link(),
            'page_links': page_links
        }

    async def to_html(self):
        template = await sync_to_async(loader.get_template)(self.template)
        context = await self.get_html_context()
        return await sync_to_async(template.render)(context)

    async def get_count(self, queryset):
        try:
            return await sync_to_async(queryset.count)()
        except (AttributeError, TypeError):
            return len(queryset)

    async def get_schema_fields(self, view):
        assert coreapi is not None, 'coreapi must be installed to use `get_schema_fields()`'
        if coreapi is not None:
            warnings.warn('CoreAPI compatibility is deprecated and will be removed in DRF 3.17')
        assert coreschema is not None, 'coreschema must be installed to use `get_schema_fields()`'
        return [
            coreapi.Field(
                name=self.limit_query_param,
                required=False,
                location='query',
                schema=coreschema.Integer(
                    title='Limit',
                    description=await force_str(self.limit_query_description)
                )
            ),
            coreapi.Field(
                name=self.offset_query_param,
                required=False,
                location='query',
                schema=coreschema.Integer(
                    title='Offset',
                    description=await force_str(self.offset_query_description)
                )
            )
        ]

    async def get_schema_operation_parameters(self, view):
        parameters = [
            {
                'name': self.limit_query_param,
                'required': False,
                'in': 'query',
                'description': await force_str(self.limit_query_description),
                'schema': {
                    'type': 'integer',
                },
            },
            {
                'name': self.offset_query_param,
                'required': False,
                'in': 'query',
                'description': await force_str(self.offset_query_description),
                'schema': {
                    'type': 'integer',
                },
            },
        ]
        return parameters


class CursorPagination(CursorPagination):
    async def paginate_queryset(self, queryset, request, view=None):
        self.request = request
        self.page_size = await self.get_page_size(request)
        if not self.page_size:
            return None

        self.base_url = await sync_to_async(request.build_absolute_uri)()
        self.ordering = await self.get_ordering(request, queryset, view)

        self.cursor = await self.decode_cursor(request)
        if self.cursor is None:
            (offset, reverse, current_position) = (0, False, None)
        else:
            (offset, reverse, current_position) = self.cursor

        if reverse:
            queryset = queryset.order_by(*await _reverse_ordering(self.ordering))
        else:
            queryset = queryset.order_by(*self.ordering)

        if str(current_position) != 'None':
            order = self.ordering[0]
            is_reversed = order.startswith('-')
            order_attr = order.lstrip('-')

            if self.cursor.reverse != is_reversed:
                kwargs = {order_attr + '__lt': current_position}
            else:
                kwargs = {order_attr + '__gt': current_position}

            filter_query = Q(**kwargs)
            if (reverse and not is_reversed) or is_reversed:
                filter_query |= Q(**{order_attr + '__isnull': True})
            queryset = queryset.filter(filter_query)

        results = list(queryset[offset:offset + self.page_size + 1])
        self.page = list(results[:self.page_size])

        if len(results) > len(self.page):
            has_following_position = True
            following_position = await self._get_position_from_instance(results[-1], self.ordering)
        else:
            has_following_position = False
            following_position = None

        if reverse:
            self.page = list(reversed(self.page))

            self.has_next = (current_position is not None) or (offset > 0)
            self.has_previous = has_following_position
            if self.has_next:
                self.next_position = current_position
            if self.has_previous:
                self.previous_position = following_position
        else:
            self.has_next = has_following_position
            self.has_previous = (current_position is not None) or (offset > 0)
            if self.has_next:
                self.next_position = following_position
            if self.has_previous:
                self.previous_position = current_position

        if (self.has_previous or self.has_next) and self.template is not None:
            self.display_page_controls = True

        return self.page

    async def get_page_size(self, request):
        if self.page_size_query_param:
            with sync_to_async(contextlib.suppress)(KeyError, ValueError):
                return await _positive_int(
                    request.query_params[self.page_size_query_param],
                    strict=True,
                    cutoff=self.max_page_size
                )
        return self.page_size

    async def get_next_link(self):
        if not self.has_next:
            return None

        if self.page and self.cursor and self.cursor.reverse and self.cursor.offset != 0:
            compare = await self._get_position_from_instance(self.page[-1], self.ordering)
        else:
            compare = self.next_position
        offset = 0

        has_item_with_unique_position = False
        for item in reversed(self.page):
            position = await self._get_position_from_instance(item, self.ordering)
            if position != compare:
                has_item_with_unique_position = position is not None
                break

            compare = position
            offset += 1

        if self.page and not has_item_with_unique_position:
            if not self.has_previous:
                offset = self.page_size
                position = None
            elif self.cursor.reverse:
                offset = 0
                position = self.previous_position
            else:
                offset = self.cursor.offset + self.page_size
                position = self.previous_position

        if not self.page:
            position = self.next_position

        cursor = Cursor(offset=offset, reverse=False, position=position)
        return await self.encode_cursor(cursor)

    async def get_previous_link(self):
        if not self.has_previous:
            return None

        if self.page and self.cursor and not self.cursor.reverse and self.cursor.offset != 0:
            compare = await self._get_position_from_instance(self.page[0], self.ordering)
        else:
            compare = self.previous_position
        offset = 0

        has_item_with_unique_position = False
        for item in self.page:
            position = await self._get_position_from_instance(item, self.ordering)
            if position != compare:
                has_item_with_unique_position = position is not None
                break

            compare = position
            offset += 1

        if self.page and not has_item_with_unique_position:
            if not self.has_next:
                offset = self.page_size
                position = None
            elif self.cursor.reverse:
                offset = self.cursor.offset + self.page_size
                position = self.next_position
            else:
                offset = 0
                position = self.next_position

        if not self.page:
            position = self.previous_position

        cursor = Cursor(offset=offset, reverse=True, position=position)
        return await self.encode_cursor(cursor)

    async def get_ordering(self, request, queryset, view):
        ordering = self.ordering

        ordering_filters = [
            filter_cls for filter_cls in getattr(view, 'filter_backends', [])
            if hasattr(filter_cls, 'get_ordering')
        ]

        if ordering_filters:
            filter_cls = ordering_filters[0]
            filter_instance = filter_cls()
            ordering_from_filter = await sync_to_async(filter_instance.get_ordering)(request, queryset, view)
            if ordering_from_filter:
                ordering = ordering_from_filter

        assert ordering is not None, (
            'Using cursor pagination, but no ordering attribute was declared '
            'on the pagination class.'
        )
        assert '__' not in ordering, (
            'Cursor pagination does not support double underscore lookups '
            'for orderings. Orderings should be an unchanging, unique or '
            'nearly-unique field on the model, such as "-created" or "pk".'
        )

        assert isinstance(ordering, (str, list, tuple)), (
            'Invalid ordering. Expected string or tuple, but got {type}'.format(
                type=type(ordering).__name__
            )
        )

        if isinstance(ordering, str):
            return (ordering,)
        return tuple(ordering)

    async def decode_cursor(self, request):
        encoded = request.query_params.get(self.cursor_query_param)
        if encoded is None:
            return None

        try:
            querystring = b64decode(encoded.encode('ascii')).decode('ascii')
            tokens = parse.parse_qs(querystring, keep_blank_values=True)

            offset = tokens.get('o', ['0'])[0]
            offset = _positive_int(offset, cutoff=self.offset_cutoff)

            reverse = tokens.get('r', ['0'])[0]
            reverse = bool(int(reverse))

            position = tokens.get('p', [None])[0]
        except (TypeError, ValueError):
            raise NotFound(self.invalid_cursor_message)

        return Cursor(offset=offset, reverse=reverse, position=position)

    async def encode_cursor(self, cursor):
        tokens = {}
        if cursor.offset != 0:
            tokens['o'] = str(cursor.offset)
        if cursor.reverse:
            tokens['r'] = '1'
        if cursor.position is not None:
            tokens['p'] = cursor.position

        querystring = parse.urlencode(tokens, doseq=True)
        encoded = b64encode(querystring.encode('ascii')).decode('ascii')
        return await replace_query_param(self.base_url, self.cursor_query_param, encoded)

    async def _get_position_from_instance(self, instance, ordering):
        field_name = ordering[0].lstrip('-')
        if isinstance(instance, dict):
            attr = instance[field_name]
        else:
            attr = getattr(instance, field_name)
        return None if attr is None else str(attr)

    async def get_paginated_response(self, data):
        return Response({
            'next': await self.get_next_link(),
            'previous': await self.get_previous_link(),
            'results': data,
        })

    async def get_paginated_response_schema(self, schema):
        return {
            'type': 'object',
            'required': ['results'],
            'properties': {
                'next': {
                    'type': 'string',
                    'nullable': True,
                    'format': 'uri',
                    'example': 'http://api.example.org/accounts/?{cursor_query_param}=cD00ODY%3D"'.format(
                        cursor_query_param=self.cursor_query_param)
                },
                'previous': {
                    'type': 'string',
                    'nullable': True,
                    'format': 'uri',
                    'example': 'http://api.example.org/accounts/?{cursor_query_param}=cj0xJnA9NDg3'.format(
                        cursor_query_param=self.cursor_query_param)
                },
                'results': schema,
            },
        }

    async def get_html_context(self):
        return {
            'previous_url': await self.get_previous_link(),
            'next_url': await self.get_next_link()
        }

    async def to_html(self):
        template = await sync_to_async(loader.get_template)(self.template)
        context = await self.get_html_context()
        return sync_to_async(template.render)(context)

    async def get_schema_fields(self, view):
        assert coreapi is not None, 'coreapi must be installed to use `get_schema_fields()`'
        if coreapi is not None:
            warnings.warn('CoreAPI compatibility is deprecated and will be removed in DRF 3.17')
        assert coreschema is not None, 'coreschema must be installed to use `get_schema_fields()`'
        fields = [
            coreapi.Field(
                name=self.cursor_query_param,
                required=False,
                location='query',
                schema=coreschema.String(
                    title='Cursor',
                    description=await force_str(self.cursor_query_description)
                )
            )
        ]
        if self.page_size_query_param is not None:
            fields.append(
                coreapi.Field(
                    name=self.page_size_query_param,
                    required=False,
                    location='query',
                    schema=coreschema.Integer(
                        title='Page size',
                        description=await force_str(self.page_size_query_description)
                    )
                )
            )
        return fields

    async def get_schema_operation_parameters(self, view):
        parameters = [
            {
                'name': self.cursor_query_param,
                'required': False,
                'in': 'query',
                'description': await force_str(self.cursor_query_description),
                'schema': {
                    'type': 'string',
                },
            }
        ]
        if self.page_size_query_param is not None:
            parameters.append(
                {
                    'name': self.page_size_query_param,
                    'required': False,
                    'in': 'query',
                    'description': await force_str(self.page_size_query_description),
                    'schema': {
                        'type': 'integer',
                    },
                }
            )
        return parameters
