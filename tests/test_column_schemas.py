from unittest.mock import Mock

import goodboy as gb
import pytest
import sqlalchemy as sa

from goodboy_sqlalchemy.column_schemas import (
    ColumnSchemaBuilder,
    ColumnSchemaBuilderError,
    SimpleColumnSchemaFactory,
    StringColumnSchemaFactory,
)


@pytest.mark.parametrize("nullable", [True, False])
def test_simple_column_schema_factory(nullable):
    column = sa.Column("dummy", sa.Integer, nullable=nullable)

    schema = object()
    schema_class = Mock(return_value=schema)

    factory = SimpleColumnSchemaFactory(schema_class)
    result = factory.build(column)

    assert result is schema
    schema_class.assert_called_once_with(allow_none=nullable)


@pytest.mark.parametrize("nullable", [True, False])
def test_string_column_schema_factory(nullable):
    column = sa.Column("dummy", sa.String(length=255), nullable=nullable)

    factory = StringColumnSchemaFactory()
    result = factory.build(column)

    assert result == gb.Str(allow_none=nullable, max_length=255)


def test_schema_builder_when_type_mapped():
    column = sa.Column("dummy", sa.Integer, nullable=False)

    schema = object()
    schema_factory = Mock()
    schema_factory.build = Mock(return_value=schema)

    builder = ColumnSchemaBuilder({sa.Integer: schema_factory})

    assert builder.build(column) is schema
    schema_factory.build.assert_called_once_with(column)


def test_schema_builder_when_type_unmapped():
    column = sa.Column("dummy", sa.Integer, nullable=False)
    builder = ColumnSchemaBuilder({})

    with pytest.raises(ColumnSchemaBuilderError):
        assert builder.build(column)
