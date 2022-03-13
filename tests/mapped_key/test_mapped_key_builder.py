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
    name = sa.Column(sa.String)


def test_builds_mapped_sa_column():
    column = Column("name", gb.Str())
    builder = MappedKeyBuilder()

    assert builder.build(Dummy, column) == MappedColumnKey(
        Dummy, Dummy.name, Dummy.id, column, DEFAULT_MESSAGES
    )


def test_raises_error_when_sa_column_not_found():
    column = Column("unknown", gb.Str())
    builder = MappedKeyBuilder()

    with pytest.raises(MappedKeyBuilderError):
        builder.build(Dummy, column)


def test_mapped_key_building():
    key = gb.Key("name", gb.Str())
    builder = MappedKeyBuilder()

    assert builder.build(Dummy, key) == MappedPropertyKey(key)
