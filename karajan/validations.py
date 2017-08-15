from karajan.exceptions import KarajanValidationException


def validate_presence(val, msg=None):
    if val is None or val == '':
        if not msg:
            msg = "%s not present" % val
        raise KarajanValidationException(msg)


def validate_absence(val, msg=None):
    if val is not None and val != '':
        if not msg:
            msg = "%s present" % val
        raise KarajanValidationException(msg)


def validate_empty(val, msg=None):
    if not isinstance(val, (list, dict)) or val:
        if not msg:
            msg = "%s not empty" % val
        raise KarajanValidationException(msg)


def validate_not_empty(val, msg=None):
    if not isinstance(val, (list, dict)) or not val:
        if not msg:
            msg = "%s empty" % val
        raise KarajanValidationException(msg)


def validate_include(items, val, msg=None):
    if not val in items:
        if not msg:
            msg = "%s not in %s" % (val, items)
        raise KarajanValidationException(msg)


def validate_exclude(items, val, msg=None):
    if val in items:
        if not msg:
            msg = "%s in %s" % (val, items)
        raise KarajanValidationException(msg)


class Validatable:
    def __init__(self):
        pass

    def validate(self):
        pass

    def _get_attr(self, attr, name=None):
        if name is None:
            name = attr
        return getattr(self, attr, None), name

    def _class_name(self):
        return self.__class__.__name__

    def validate_presence(self, attr, name=None):
        val, name = self._get_attr(attr, name)
        validate_presence(val, "%s: %s must be present" % (self._class_name(), name))

    def validate_absence(self, attr, name=None):
        val, name = self._get_attr(attr, name)
        validate_absence(val, "%s: %s must not be present" % (self._class_name(), name))

    def validate_empty(self, attr, name=None):
        val, name = self._get_attr(attr, name)
        validate_empty(val, "%s: %s must be empty" % (self._class_name(), name))

    def validate_not_empty(self, attr, name=None):
        val, name = self._get_attr(attr, name)
        validate_not_empty(val, "%s: %s must not be empty" % (self._class_name(), name))

    def validate_include(self, attr, val, name=None):
        items, name = self._get_attr(attr, name)
        validate_include(items, val, "%s: %s must contain %s" % (self._class_name(), name, val))

    def validate_exclude(self, attr, val, name=None):
        items, name = self._get_attr(attr, name)
        validate_exclude(items, val, "%s: %s must not contain %s" % (self._class_name(), name, val))

    def validate_in(self, attr, items, name=None):
        val, name = self._get_attr(attr, name)
        validate_include(items, val, "%s: %s must be one of %s" % (self._class_name(), name, items))

    def validate_not_in(self, attr, items, name=None):
        val, name = self._get_attr(attr, name)
        validate_exclude(items, val, "%s: %s must not be one of %s" % (self._class_name(), name, items))
