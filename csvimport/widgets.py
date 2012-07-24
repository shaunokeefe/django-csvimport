from django.utils.safestring import mark_safe
from django.utils.encoding import force_unicode
from django.utils.html import linebreaks

from django.forms import Textarea
from django.forms.util import flatatt

class ErrorTextarea(Textarea):
    def render(self, name, value, attrs=None):
        if value is None: value = ''
        final_attrs = self.build_attrs(attrs, name=name)
        return mark_safe(u'<p%s>%s</p>' % (flatatt(final_attrs),
            force_unicode(linebreaks(value))))
