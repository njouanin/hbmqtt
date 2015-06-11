__author__ = 'nico'


class A:
    def __init__(self):
        self.a = 0



def do(a):
    a.a = 1



if __name__ == '__main__':
    a=A()
    print(a.a)
    do(a)
    print(a.a)
