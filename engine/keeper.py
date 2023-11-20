import weakref
from collections import defaultdict


class InstanceKeeper(object):
    __refs__ = defaultdict(list)

    def register(self, cls_name: str, obj: object):
        self.__refs__[cls_name].append(weakref.ref(obj))

    @classmethod
    def get_instances(cls, cls_name: str):
        for inst_ref in cls.__refs__[cls_name]:
            inst = inst_ref()
            if inst is not None:
                yield inst


keeper = InstanceKeeper()

