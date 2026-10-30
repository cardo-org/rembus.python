from rembus.router import getargs


def test_getargs():
    assert getargs([1, 2]) == [1, 2]
    assert getargs([]) == []
    assert getargs(None) == []
    assert getargs(5) == [5]
    assert getargs({"a": 1}) == [{"a": 1}]
