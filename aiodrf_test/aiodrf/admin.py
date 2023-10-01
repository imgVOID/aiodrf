from django.contrib import admin
from .models import Test, TestIncluded


class TestAdmin(admin.ModelAdmin):
    pass


class TestIncludedAdmin(admin.ModelAdmin):
    pass


admin.site.register(Test, TestAdmin)
admin.site.register(TestIncluded, TestIncludedAdmin)
