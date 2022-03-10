from __future__ import annotations

from contextlib import contextmanager

import pytest
from goodboy.errors import Error
from goodboy.schema import SchemaError


@contextmanager
def assert_errors(errors: list[Error]):
    with pytest.raises(SchemaError) as e:
        yield

    assert e.value.errors == errors


@contextmanager
def assert_dict_key_errors(key_errors: dict[str, list[Error]]):
    with assert_errors([Error("key_errors", nested_errors=key_errors)]):
        yield


@contextmanager
def assert_dict_value_errors(value_errors: dict[str, list[Error]]):
    with assert_errors([Error("value_errors", nested_errors=value_errors)]):
        yield
