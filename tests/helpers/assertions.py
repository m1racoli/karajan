from nose.tools import assert_equal


def assert_str_equal(actual, expected, strip=True):
    actual = actual.split('\n')
    expected = expected.split('\n')
    for al, el in zip(actual, expected):
        if strip:
            al = al.strip()
            el = el.strip()
        assert_equal(al, el)
