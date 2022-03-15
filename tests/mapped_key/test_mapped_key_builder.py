import goodboy as gb
import pytest
import sqlalchemy as sa

from goodboy_sqlalchemy.column import Column
from goodboy_sqlalchemy.mapped_key import (
    MappedColumnKey,
    MappedKeyBuilder,
    MappedKeyBuilderError,
    MappedPropertyKey,
)
from goodboy_sqlalchemy.messages import DEFAULT_MESSAGES

Base = sa.orm.declarative_base()


class Dummy(Base):
    __tablename__ = "dummies"

    id = sa.Column(sa.Integer, primary_key=True)

    field_1 = sa.Column(sa.String, nullable=False, unique=True)
    field_2 = sa.Column(sa.String)


@pytest.fixture
def mapped_key_builder():
    return MappedKeyBuilder()


def test_builds_mapped_column_keys(mapped_key_builder: MappedKeyBuilder):
    keys = [
        Column("field_1", gb.Str(), required=True, unique=True),
        Column("field_2", gb.Str(allow_none=True), required=False, unique=False),
    ]

    assert mapped_key_builder.build(Dummy, keys) == [
        MappedColumnKey(Dummy, Dummy.field_1, Dummy.id, keys[0], DEFAULT_MESSAGES),
        MappedColumnKey(Dummy, Dummy.field_2, Dummy.id, keys[1], DEFAULT_MESSAGES),
    ]


def test_raises_error_when_column_not_found(mapped_key_builder: MappedKeyBuilder):
    keys = [
        Column("unknown_field", gb.Str()),
    ]

    with pytest.raises(MappedKeyBuilderError):
        mapped_key_builder.build(Dummy, keys)


def test_builds_mapped_property_keys(mapped_key_builder: MappedKeyBuilder):
    keys = [
        gb.Key("property_1"),
        gb.Key("property_2"),
    ]

    assert mapped_key_builder.build(Dummy, keys) == [
        MappedPropertyKey(keys[0]),
        MappedPropertyKey(keys[1]),
    ]
