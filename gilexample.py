import sys

a = []
b = a
c = a

ekrem = a
print(sys.getrefcount(a)) #if refcount reduces to 0, garbage collector cleans it up