from unittest import mock

import goodboy as gb
import sqlalchemy as sa

from goodboy_sqlalchemy.column import Column, MappedColumn, MappedColumnBuilder
from goodboy_sqlalchemy.messages import DEFAULT_MESSAGES

Base = sa.orm.declarative_base()


class Dummy(Base):
    __tablename__ = "dummies"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)


def test_column_and_primary_key_lookup():
    column = Column("name", gb.Str())
    builder = MappedColumnBuilder()

    with mock.patch.object(MappedColumn, "__init__", return_value=None) as mocked_init:
        builder.build(Dummy, column)

    mocked_init.assert_called_once_with(
        Dummy,
        Dummy.name,
        Dummy.id,
        column,
        DEFAULT_MESSAGES,
    )
