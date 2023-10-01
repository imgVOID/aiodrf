# Generated by Django 4.2.5 on 2023-09-30 13:49

import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('aiodrf', '0002_remove_test_array'),
    ]

    operations = [
        migrations.AddField(
            model_name='test',
            name='array',
            field=django.contrib.postgres.fields.ArrayField(base_field=models.IntegerField(), default=list, size=2),
        ),
    ]
