import os
import sys
import importlib

from typing import Any


def _resolve_obj_rec(name: str) -> Any:
    try:
        return importlib.import_module(name)
    except ImportError:
        if "." not in name:  # no chance
            raise
    ns_name, obj_name = name.rsplit(".", 1)
    mod = _resolve_obj_rec(ns_name)
    return getattr(mod, obj_name)


def resolve_obj(name: str) -> Any:
    orig_sys_path = list(sys.path)
    try:
        sys.path.append(os.getcwd())
        return _resolve_obj_rec(name)
    finally:
        sys.path.clear()
        sys.path.extend(orig_sys_path)

