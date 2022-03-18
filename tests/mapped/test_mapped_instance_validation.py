from datetime import date

import goodboy as gb
import pytest
import sqlalchemy as sa

from goodboy_sqlalchemy.column import Column
from goodboy_sqlalchemy.mapped import Mapped
from tests.conftest import assert_dict_key_errors, assert_dict_value_errors

# Use in-memory SQLite
engine = sa.create_engine("sqlite://")
Session = sa.orm.sessionmaker(engine)
Base = sa.orm.declarative_base()


class User(Base):
    __tablename__ = "users"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False, unique=True)
    bday = sa.Column(sa.Date)
    marty_stuff = sa.Column(sa.String)


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


@pytest.fixture()
def user_mapped():
    return Mapped(
        User,
        keys=[
            Column("name", gb.Str(), required=True, unique=True),
            Column("bday", gb.Date(allow_none=True), required=False),
        ],
    )


def test_accepts_good_value_without_instance(user_mapped, context):
    good_value = {"name": "Marty", "bday": date(1968, 6, 12)}
    assert user_mapped(good_value, context=context) == good_value


@pytest.mark.parametrize(
    "good_value",
    [
        {"name": "Marty", "bday": date(1968, 6, 12)},
        {"bday": date(1968, 6, 12)},
        {},
    ],
)
def test_accepts_good_value_with_instance(user_mapped, session, good_value):
    user = User(name="Marty", bday=date(1968, 6, 12))

    session.add(user)
    session.flush()

    context = {"session": session, "mapped_instance": user}

    assert user_mapped(good_value, context=context) == good_value


def test_rejects_bad_value_without_instance(user_mapped, context):
    with assert_dict_key_errors({"name": [gb.Error("required_key")]}):
        user_mapped({}, context=context)

    with assert_dict_value_errors({"name": [gb.Error("cannot_be_blank")]}):
        user_mapped({"name": ""}, context=context)


def test_rejects_bad_value_with_instance(user_mapped, session):
    user = User(name="Marty", bday=date(1968, 6, 12))

    session.add(user)
    session.flush()

    context = {"session": session, "mapped_instance": user}

    with assert_dict_value_errors({"name": [gb.Error("cannot_be_blank")]}):
        user_mapped({"name": ""}, context=context)

    with assert_dict_value_errors({"name": [gb.Error("cannot_be_none")]}):
        user_mapped({"name": None}, context=context)


def test_rejects_non_unique_value_of_new_mapped_instance(user_mapped, session):
    session.add(User(name="Marty"))
    session.flush()

    context = {"session": session}

    with assert_dict_value_errors({"name": [gb.Error("already_exists")]}):
        user_mapped({"name": "Marty"}, context=context)


def test_accepts_non_unique_value_of_old_mapped_instance(user_mapped, session):
    user = User(name="Marty")

    session.add(user)
    session.flush()

    context = {"session": session, "mapped_instance": user}
    good_value = {"name": "Marty"}

    assert user_mapped(good_value, context=context) == good_value


@pytest.fixture()
def user_conditional_mapped():
    return Mapped(
        User,
        keys=[
            Column("name", gb.Str(), required=True, unique=True),
            Column(
                "marty_stuff",
                gb.Str(),
                required=False,
                predicate=lambda u: u["name"] == "Marty",
            ),
        ],
    )


def test_conditional_validation_when_instance_specified(
    user_conditional_mapped, session
):
    user = User(name="Marty")
    context = {"session": session, "mapped_instance": user}

    good_value_1 = {"name": "Marty", "marty_stuff": "woo-hoo"}
    good_value_2 = {"marty_stuff": "woo-hoo"}
    good_value_3 = {"name": "Doc"}

    assert user_conditional_mapped(good_value_1, context=context) == good_value_1
    assert user_conditional_mapped(good_value_2, context=context) == good_value_2
    assert user_conditional_mapped(good_value_3, context=context) == good_value_3

    with assert_dict_key_errors({"marty_stuff": [gb.Error("unknown_key")]}):
        user_conditional_mapped(
            {"name": "Doc", "marty_stuff": "woo-hoo"}, context=context
        )


def test_conditional_validation_when_no_instance_specified(
    user_conditional_mapped, session
):
    context = {"session": session}

    good_value_1 = {"name": "Marty", "marty_stuff": "woo-hoo"}
    good_value_2 = {"name": "Doc"}

    assert user_conditional_mapped(good_value_1, context=context) == good_value_1
    assert user_conditional_mapped(good_value_2, context=context) == good_value_2

    with assert_dict_key_errors({"marty_stuff": [gb.Error("unknown_key")]}):
        user_conditional_mapped(
            {"name": "Doc", "marty_stuff": "woo-hoo"}, context=context
        )
