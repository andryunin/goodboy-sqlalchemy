from datetime import date

import goodboy as gb
import pytest
import sqlalchemy as sa

from goodboy_sqlalchemy.column import Column
from goodboy_sqlalchemy.mapped import Mapped
from tests.conftest import (
    assert_dict_key_errors,
    assert_dict_value_errors,
    assert_errors,
)

# Use in-memory SQLite
engine = sa.create_engine("sqlite://")
Session = sa.orm.sessionmaker(engine)
Base = sa.orm.declarative_base()


class User(Base):
    __tablename__ = "users"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)


Base.metadata.create_all(engine)


@pytest.fixture()
def session():
    try:
        session = Session()
        yield session
    finally:
        session.rollback()


@pytest.fixture()
def context(session):
    return {"session": session}


def test_accepts_dict_type(context):
    assert Mapped(User)({}, context=context) == {}


def test_rejects_non_dict_type(context):
    with assert_errors(
        [gb.Error("unexpected_type", {"expected_type": gb.type_name("dict")})]
    ):
        Mapped(User)(42, context=context)


def test_accepts_with_optional_key(context):
    schema = Mapped(User, keys=[gb.Key("minor_key", required=False)])
    good_value = {"minor_key": None}

    assert schema(good_value, context=context) == good_value


def test_accepts_without_optional_key(context):
    schema = Mapped(User, keys=[gb.Key("minor_key", required=False)])
    assert schema({}, context=context) == {}


def test_accepts_with_required_key(context):
    schema = Mapped(User, keys=[gb.Key("major_key", required=True)])
    good_value = {"major_key": None}

    assert schema(good_value, context=context) == good_value


def test_rejects_without_required_key(context):
    schema = Mapped(User, keys=[gb.Key("major_key", required=True)])

    with assert_dict_key_errors({"major_key": [gb.Error("required_key")]}):
        schema({}, context=context)


def test_rejects_unknown_key(context):
    schema = Mapped(User)

    with assert_dict_key_errors({"oops": [gb.Error("unknown_key")]}):
        schema({"oops": True}, context=context)


def test_default_value_for_absent_key(context):
    schema = Mapped(User, keys=[gb.Key("default_key", default="yeah")])
    assert schema({}, context=context) == {"default_key": "yeah"}


def test_ignores_default_value_for_present_key_with_none_value(context):
    schema = Mapped(User, keys=[gb.Key("default_key", default="foo")])
    assert schema({"default_key": None}, context=context) == {"default_key": None}


def test_passes_typecast_flag_to_key_schemas(context):
    mapped = Mapped(User, keys=[gb.Key("date", gb.Date())])

    value = {"date": "1968-06-12"}
    value_casted = {"date": date(1968, 6, 12)}

    assert mapped(value, typecast=True, context=context) == value_casted

    with assert_dict_value_errors(
        {"date": [gb.Error("unexpected_type", {"expected_type": gb.type_name("date")})]}
    ):
        mapped(value, context=context)


def test_rejects_values_with_typecasting_errors(context):
    mapped = Mapped(User, keys=[gb.Key("date", gb.Date())])
    bad_value = {"date": "1970/01/01"}

    with assert_dict_value_errors({"date": [gb.Error("invalid_date_format")]}):
        mapped(bad_value, typecast=True, context=context)


@pytest.mark.parametrize(
    "good_value",
    [
        {"field": "name", "val": "Marty"},
        {"field": "birthday", "val": date(1968, 6, 12)},
    ],
)
def test_accepts_values_when_conditional_validation_succeed(good_value, context):
    schema = Mapped(
        User,
        keys=[
            gb.Key("field", gb.Str()),
            gb.Key("val", gb.Str(), predicate=lambda d: d.get("field") == "name"),
            gb.Key("val", gb.Date(), predicate=lambda d: d.get("field") == "birthday"),
        ],
    )

    assert schema(good_value, context=context) == good_value


@pytest.mark.parametrize(
    "bad_value,type_name",
    [
        ({"field": "name", "val": date(1968, 6, 12)}, gb.type_name("str")),
        ({"field": "birthday", "val": "Marty"}, gb.type_name("date")),
    ],
)
def test_rejects_values_when_conditional_validation_failed(
    bad_value, type_name, context
):
    schema = Mapped(
        User,
        keys=[
            gb.Key("field", gb.Str()),
            gb.Key("val", gb.Str(), predicate=lambda d: d.get("field") == "name"),
            gb.Key("val", gb.Date(), predicate=lambda d: d.get("field") == "birthday"),
        ],
    )

    with assert_dict_value_errors(
        {"val": [gb.Error("unexpected_type", {"expected_type": type_name})]}
    ):
        schema(bad_value, context=context)


def test_replaces_column_name_with_(context):
    schema = Mapped(
        User,
        keys=[
            Column("nickname", gb.Str(allow_none=True), mapped_column_name="name"),
        ],
    )

    assert schema({"nickname": "Marty"}, context=context) == {"name": "Marty"}
    assert schema({"nickname": None}, context=context) == {"name": None}


def test_merges_value_errors_from_rule_errors(context):
    schema = Mapped(
        User,
        rules=[rule_with_value_errors],
        keys=[
            Column(
                "nickname", gb.Str(allowed=["Marty", "Doc"]), mapped_column_name="name"
            ),
        ],
    )

    with assert_errors(
        [
            gb.Error(
                "value_errors",
                nested_errors={
                    "nickname": [
                        gb.Error("not_allowed", {"allowed": ["Marty", "Doc"]}),
                        gb.Error("value_error_from_rule"),
                    ],
                },
            )
        ]
    ):
        schema({"nickname": "Rick"}, context=context)


def rule_with_key_errors(self: Mapped, value, typecast: bool, context: dict):
    return value, [
        self._error(
            "key_errors",
            nested_errors={"nickname": [self._error("key_error_from_rule")]},
        )
    ]


def rule_with_value_errors(self: Mapped, value, typecast: bool, context: dict):
    return value, [
        self._error(
            "value_errors",
            nested_errors={"nickname": [self._error("value_error_from_rule")]},
        ),
    ]
