# Thanks to the adrf package creators
import asyncio
from functools import update_wrapper

from django.utils.decorators import classonlymethod
from django.utils.functional import classproperty

from adrf.views import APIView
from rest_framework.viewsets import ViewSetMixin as DRFViewSetMixin


class ViewSetMixin(DRFViewSetMixin):

    @classonlymethod
    def as_view(cls, actions=None, **initkwargs):
        cls.name, cls.description, cls.suffix = None, None, None
        cls.detail, cls.basename = None, None

        if not actions:
            raise TypeError("The `actions` argument must be provided when "
                            "calling `.as_view()` on a ViewSet. For example "
                            "`.as_view({'get': 'list'})`")

        for key in initkwargs:
            if key in cls.http_method_names:
                raise TypeError("You tried to pass in the %s method name as a "
                                "keyword argument to %s(). Don't do that."
                                % (key, cls.__name__))
            if not hasattr(cls, key):
                raise TypeError("%s() received an invalid keyword %r" % (
                    cls.__name__, key))

        if 'name' in initkwargs and 'suffix' in initkwargs:
            raise TypeError(
                "%s() received both `name` and `suffix`, which are "
                "mutually exclusive arguments." % (cls.__name__))

        def view(request, *args, **kwargs):
            self = cls(**initkwargs)

            if 'get' in actions and 'head' not in actions:
                actions['head'] = actions['get']

            self.action_map = actions

            for method, action in actions.items():
                handler = getattr(self, action)
                setattr(self, method, handler)

            self.request, self.args, self.kwargs = request, args, kwargs

            return self.dispatch(request, *args, **kwargs)

        async def async_view(request, *args, **kwargs):
            self = cls(**initkwargs)

            if 'get' in actions and 'head' not in actions:
                actions['head'] = actions['get']

            self.action_map = actions

            for method, action in actions.items():
                handler = getattr(self, action)
                setattr(self, method, handler)

            self.request, self.args, self.kwargs = request, args, kwargs

            return await self.dispatch(request, *args, **kwargs)

        view = async_view if cls.view_is_async else view
        update_wrapper(view, cls, updated=())
        update_wrapper(view, cls.dispatch, assigned=())
        view.cls, view.initkwargs = cls, initkwargs
        view.actions, view.csrf_exempt = actions, True
        return view


class ViewSet(ViewSetMixin, APIView):

    @classproperty
    def view_is_async(cls):
        result = [
            asyncio.iscoroutinefunction(function)
            for name, function in cls.__dict__.items()
            if callable(function) and not name.startswith("__")
        ]
        return any(result)
