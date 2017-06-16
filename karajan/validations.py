class ValidationException(Exception):
    pass


def validate_presence(o, attr):
    val = getattr(o, attr, None)
    if not val:
        raise ValidationException("%s: %s must be present" % (type(o), attr))


def validate_not_empty(o, attr):
    val = getattr(o, attr, None)
    if not isinstance(val, (list, dict)) or not val:
        raise ValidationException("%s: %s must be non-empty" % (type(o), attr))
