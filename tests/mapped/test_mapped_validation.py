from datetime import date

import goodboy as gb
import pytest
import sqlalchemy as sa

from goodboy_sqlalchemy.column import Column
from goodboy_sqlalchemy.mapped import Mapped
from goodboy_sqlalchemy.messages import DEFAULT_MESSAGES
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
    name = sa.Column(sa.String, nullable=False, unique=True)
    bday = sa.Column(sa.Date)


Base.metadata.create_all(engine)


@pytest.fixture()
def session():
    try:
        session = Session()
        yield session
    finally:
        session.rollback()


def test_accepts_good_value(session):
    mapped = Mapped(User, column_names=["name", "bday"])
    good_value = {"name": "Marty", "bday": date(1968, 6, 12)}
    assert mapped(good_value, context={"session": session}) == good_value


def test_rejects_bad_value(session):
    mapped = Mapped(User, column_names=["name", "bday"])

    with assert_dict_key_errors({"name": [gb.Error("required_key")]}):
        mapped({}, context={"session": session})
