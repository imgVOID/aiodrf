from .serializers import ModelSerializerAsync
from .models import Test
from . import generics
from rest_framework import generics
from rest_framework.permissions import IsAdminUser


class SerializerTest(ModelSerializerAsync):
    class Meta:
        model, fields = Test, '__all__'

class TestList(generics.ListCreateAPIView):
    queryset = Test.objects.all()
    serializer_class = SerializerTest
    permission_classes = [IsAdminUser]
