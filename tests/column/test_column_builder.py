from unittest.mock import Mock

from goodboy import Int
from sqlalchemy import Column as SAColumn
from sqlalchemy import Integer

from goodboy_sqlalchemy.column import ColumnBuilder
from goodboy_sqlalchemy.column_schemas import column_schema_builder


def test_built_column_name():
    builder = ColumnBuilder(column_schema_builder)
    column = SAColumn("dummy", Integer)
    assert builder.build(column).name == "dummy"


def test_built_column_required_flag():
    builder = ColumnBuilder(column_schema_builder)

    sa_column_nullable = SAColumn(Integer, nullable=True)
    sa_column_not_nullable = SAColumn(Integer, nullable=False)

    assert not builder.build(sa_column_nullable).required
    assert builder.build(sa_column_not_nullable).required


def test_built_column_unique_flag():
    builder = ColumnBuilder(column_schema_builder)

    sa_column_unique = SAColumn(Integer, unique=True)
    sa_column_not_unique = SAColumn(Integer, unique=False)

    assert builder.build(sa_column_unique).unique
    assert not builder.build(sa_column_not_unique).unique


def test_column_schema_builder_is_used():
    column_schema_builder_mock = Mock()
    column_schema_builder_mock.build = Mock()

    column = SAColumn("dummy", Integer)

    builder = ColumnBuilder(column_schema_builder_mock)
    builder.build(column)

    column_schema_builder_mock.build.assert_called_once_with(column)
