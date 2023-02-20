import importlib
import inspect
import os
from typing import Collection


def get_modules_in_package(package_name: str) -> Collection:
    files = os.listdir(package_name)
    for file in files:
        if file not in ['__init__.py', '__pycache__']:
            if file[-3:] != '.py':
                continue

            file_name = file[:-3]
            module_name = package_name + '.' + file_name
            for name, cls in inspect.getmembers(importlib.import_module(module_name), inspect.isclass):
                if cls.__module__ == module_name:
                    yield name, cls
