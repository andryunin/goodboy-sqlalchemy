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
    field_3 = sa.Column(sa.String, default="val")
    field_4 = sa.Column(sa.String, server_default="val")
    field_5 = sa.Column("field_5_in_database", sa.String)


@pytest.fixture
def column_builder():
    return ColumnBuilder(column_schema_builder)


def test_builds_simple_columns(column_builder: ColumnBuilder):
    assert column_builder.build(Dummy, ["field_1", "field_2"]) == [
        Column("field_1", gb.Str(), required=True, unique=True),
        Column("field_2", gb.Str(allow_none=True), required=False, unique=False),
    ]


def test_builds_renamed_columns(column_builder: ColumnBuilder):
    print(column_builder.build(Dummy, ["field_5"])[0])
    print(column_builder.build(Dummy, ["field_5"])[0].__dict__)
    assert column_builder.build(Dummy, ["field_5"]) == [
        Column("field_5", gb.Str(allow_none=True), required=False, unique=False),
    ]


def test_handles_default_value(column_builder: ColumnBuilder):
    column = Column(
        "field_3",
        gb.Str(allow_none=True),
        required=False,
        unique=False,
        default="val",
    )

    assert column_builder.build(Dummy, ["field_3"]) == [column]


def test_handles_server_default_value(column_builder: ColumnBuilder):
    column = Column(
        "field_4",
        gb.Str(allow_none=True),
        required=False,
        unique=False,
        has_default=True,
    )

    assert column_builder.build(Dummy, ["field_4"]) == [column]


def test_raises_error_when_column_not_found(column_builder: ColumnBuilder):
    with pytest.raises(ColumnBuilderError):
        column_builder.build(Dummy, ["unknown_field"])
