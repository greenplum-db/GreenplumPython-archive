from functools import singledispatch

# from greenplumpython.util import overload
from typing import overload

"""
import time
from functools import singledispatchmethod, singledispatch


class Date:
    def __init__(self):
        print("iiiiiiii")

    @overload
    def __getitem__(self, item:int):
      print("int")

    @overload
    def __getitem__(self, item:str):
      print("str")


if __name__ == "__main__":

    # Overloaded __init__
    d = Date()
    d[12]
    d["si"]


"""


@singledispatch
def _area(l, b):
    print("function")
    if isinstance(l, int):
        print("function with int")
        return l * b
    if isinstance(l, str):
        print("function with str")
        return f"({l}, {b})"


@overload
def area(l: int, b: int):
    ...


@overload
def area(l: str, b: str):
    ...


def area(l, b):
    return _area(l, b)


print(area(3, 4))
print(area("3.0", "4"))
