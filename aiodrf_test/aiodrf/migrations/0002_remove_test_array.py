# Generated by Django 4.2.5 on 2023-09-30 13:43

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('aiodrf', '0001_initial'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='test',
            name='array',
        ),
    ]