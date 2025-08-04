class ClassProperty:
    """Allows a property to be accessed like a classmethod"""

    def __init__(self, fget):
        self.fget = fget

    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)
