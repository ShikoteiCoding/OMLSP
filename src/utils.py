import ctypes


class MutableInteger(ctypes.Structure):
    _fields_ = [("value", ctypes.c_int)]

    def __init__(self, initial_value=0):
        self.value = initial_value

    def increment(self):
        self.value += 1

    def decrement(self):
        self.value -= 1

    def __str__(self):
        return str(self.value)
