import goodboy as gb
import pytest
import sqlalchemy as sa

from goodboy_sqlalchemy.column import Column, ColumnBuilder, ColumnBuilderError
from goodboy_sqlalchemy.column_schemas import column_schema_builder

Base = sa.orm.declarative_base()


class Dummy(Base):
    __tablename__ = "dummies"

    id = sa.Column(sa.Integer, primary_key=True)

    field_1 = sa.Column(sa.String, nullable=False, unique=True)
    field_2 = sa.Column(sa.String)


@pytest.fixture
def column_builder():
    return ColumnBuilder(column_schema_builder)


def test_builds_columns(column_builder: ColumnBuilder):
    assert column_builder.build(Dummy, ["field_1", "field_2"]) == [
        Column("field_1", gb.Str(), required=True, unique=True),
        Column("field_2", gb.Str(allow_none=True), required=False, unique=False),
    ]


def test_raises_error_when_column_not_found(column_builder: ColumnBuilder):
    with pytest.raises(ColumnBuilderError):
        column_builder.build(Dummy, ["unknown_field"])
