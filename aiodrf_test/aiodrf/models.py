from django.db import models
from django.contrib.postgres.fields import ArrayField
from django.utils.translation import gettext_lazy as _


class TestIncludedRelation(models.Model):
    text_included_relation = models.CharField(max_length=128, default='')
    int_included_relation = models.IntegerField(default=1)
    bool_included_relation = models.BooleanField(default=False)
    choice_int_included_relation = models.IntegerField(choices=((1, 'One'), (2, 'Two')), default=1)
    choice_str_included_relation = models.CharField(max_length=9, choices=(
        ('UK', 'United Kingdom'), ('US', 'United States')
    ), default='UK')
    
    class Meta:
        verbose_name = _('Test Included Relation')
        verbose_name_plural = _('Test Included Relations')
        db_table = default_related_name = 'test_included_relation'
    
    def __str__(self):
        return self.text_included_relation


class TestIncluded(models.Model):
    text_included = models.CharField(max_length=128, default='')
    int_included = models.IntegerField(default=1)
    bool_included = models.BooleanField(default=False)
    choice_int_included = models.IntegerField(choices=((1, 'One'), (2, 'Two')), default=1)
    choice_str_included = models.CharField(max_length=9, choices=(
        ('UK', 'United Kingdom'), ('US', 'United States')
    ), default='UK')
    foreign_key_included = models.ForeignKey(TestIncludedRelation, on_delete=models.SET_NULL, null=True, blank=True, related_name='test_included_relation')
    many_to_many_included = models.ManyToManyField(TestIncludedRelation, blank=True, related_name='test_included_relation_many')
    
    class Meta:
        verbose_name = _('Test Included')
        verbose_name_plural = _('Test Included')
        db_table = default_related_name = 'test_included'
    
    def __str__(self):
        return self.text_included

# TODO: create the MultipleChoiceField from the module or the ArrayField
class Test(models.Model):
    text = models.CharField(max_length=128, default='')
    int = models.IntegerField(default=1)
    bool = models.BooleanField(default=False)
    choice_int = models.IntegerField(choices=((1, 'One'), (2, 'Two')), default=1)
    choice_str = models.CharField(max_length=9, choices=(
        ('UK', 'United Kingdom'), ('US', 'United States')
    ), default='UK')
    foreign_key = models.ForeignKey(TestIncluded, on_delete=models.SET_NULL, null=True, blank=True, related_name='test_included')
    many_to_many = models.ManyToManyField(TestIncluded, blank=True, related_name='test_included_many')
    class Meta:
        verbose_name = _('Test')
        verbose_name_plural = _('Tests')
        default_related_name = 'test'
    
    def __str__(self):
        return self.text


class TestDirectCon(models.Model):
    text = models.CharField(max_length=128, default='')
    int = models.IntegerField(default=1)
    bool = models.BooleanField(default=False)
    choice_int = models.IntegerField(choices=((1, 'One'), (2, 'Two')), default=1)
    choice_str = models.CharField(max_length=9, choices=(
        ('UK', 'United Kingdom'), ('US', 'United States')
    ), default='UK')
    foreign_key = models.ForeignKey(TestIncluded, on_delete=models.SET_NULL, null=True, blank=True, related_name='test_dir_test_included')
    class Meta:
        verbose_name = _('Test')
        verbose_name_plural = _('Tests')
        default_related_name = 'test'
    
    def __str__(self):
        return self.text
