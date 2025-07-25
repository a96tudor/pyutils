from pyutils.decorators.singleton import singleton


@singleton
class Dummy:
    pass


def test_singleton_same_instance():
    a = Dummy()
    b = Dummy()
    assert a is b
