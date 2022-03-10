from datetime import date

import pytest
from goodboy import Date, Str
from goodboy.errors import Error
from sqlalchemy.orm import Session

from goodboy_sqlalchemy.columns import Column
from goodboy_sqlalchemy.mapped import Mapped
from tests.conftest import assert_dict_key_errors, assert_dict_value_errors


class DummyMappedClass:
    pass


@pytest.fixture
def dummy_mapped():
    return Mapped(
        DummyMappedClass,
        columns=[
            Column("name", Str(), required=True),
            Column("bday", Date(), required=True),
        ],
    )


@pytest.fixture
def session_context():
    return {"session": Session()}


def test_accepts_good_value(dummy_mapped, session_context):
    good_value = {"name": "Dummy", "bday": date(2000, 1, 1)}
    assert dummy_mapped(good_value, context=session_context) == good_value


def test_rejects_value_without_required_keys(dummy_mapped, session_context):
    with assert_dict_key_errors(
        {"name": [Error("required_key")], "bday": [Error("required_key")]},
    ):
        dummy_mapped({}, context=session_context)


def test_rejects_value_without_required_values(dummy_mapped, session_context):
    bad_value = {"name": None, "bday": None}

    with assert_dict_value_errors(
        {"name": [Error("cannot_be_none")], "bday": [Error("cannot_be_none")]},
    ):
        dummy_mapped(bad_value, context=session_context)
