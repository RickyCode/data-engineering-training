import sys

from numpy import subtract

# sys.path.append(r'C:\Users\rdelr\archivos\repos\kranio\data-engineering-training\classes-content\2022-04-08-deployments')
sys.path.append(r'/mnt/c/Users/rdelr/archivos/repos/kranio/data-engineering-training/classes-content/2022-04-08-deployments')

from example_module import add, substract, multiplicate


def test_add():
    assert add(5,3) == 8

def test_substract():
    assert subtract(8,3) == 5


# def test_failure():
#     assert False