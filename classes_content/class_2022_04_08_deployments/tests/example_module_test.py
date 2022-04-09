import sys

from classes_content.class_2022_04_08_deployments.example_module import add, substract, multiplicate


def test_add():
    assert add(5,3) == 8

def test_substract():
    assert substract(8,3) == 5


# def test_failure():
#     assert False
