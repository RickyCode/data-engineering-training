import sys
from pathlib import Path
import os

print('****', os.getcwd())
print('****', os.listdir())
print('****', sys.path)


# from classes_content.class_2022_04_08_deployments.example_module import add, substract, multiplicate
# from classes_content.class_2022_04_08_deployments.example_module import add, substract, multiplicate

from src.example_module import add, substract, divide


def test_add():
    assert add(5,3) == 8

def test_substract():
    assert substract(8,3) == 5

def test_divite():
    assert divide(8,4) == 3

print('yuhuu')


# def test_failure():
#     assert False
