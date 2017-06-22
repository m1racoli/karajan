def assert_equals(actual, expected):
    assert actual == expected, "expected %s, but got %s" % (expected, actual)


def assert_contains(l, item):
    assert item in l, "%s not found in %s" % (item, l)


def assert_contains_not(l, item):
    assert item not in l, "%s found in %s" % (item, l)
