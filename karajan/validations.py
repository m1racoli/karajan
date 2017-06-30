class ValidationException(Exception):
    pass


def validate_presence(o, attr, conf_name=None):
    if conf_name is None:
        conf_name = attr
    val = getattr(o, attr, None)
    if not val:
        raise ValidationException("%s: %s must be present" % (type(o), conf_name))


def validate_not_empty(o, attr, conf_name=None):
    if conf_name is None:
        conf_name = attr
    val = getattr(o, attr, None)
    if not isinstance(val, (list, dict)) or not val:
        raise ValidationException("%s: %s must be non-empty" % (type(o), conf_name))


def validate_in(o, items, attr, conf_name=None):
    if conf_name is None:
        conf_name = attr
    val = getattr(o, attr, None)
    if val not in items:
        raise ValidationException("%s: %s must be on of %s" % (type(o), conf_name, items))
