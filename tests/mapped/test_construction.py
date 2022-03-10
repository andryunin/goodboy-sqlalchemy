import pytest
import sqlalchemy as sa
from goodboy import Date, Dict, Key, Str
from sqlalchemy.orm import declarative_base

from goodboy_sqlalchemy.columns import Column
from goodboy_sqlalchemy.mapped import Mapped


@pytest.fixture
def Base():
    return declarative_base()


def test_basic_fields_building(Base):
    class User(Base):
        __tablename__ = "users"

        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String, nullable=False)
        bday = sa.Column(sa.Date)

    mapped = Mapped(User, column_names=["name", "bday"])

    assert mapped._dict_schema == Dict(
        keys=[
            Key("name", Str(), required=True),
            Key("bday", Date(allow_none=True), required=False),
        ]
    )
