from __future__ import annotations

from typing import Any, Callable, Optional

import goodboy as gb
import sqlalchemy as sa
import sqlalchemy.orm as sa_orm

from goodboy_sqlalchemy.column_schemas import ColumnSchemaBuilder, column_schema_builder
from goodboy_sqlalchemy.messages import DEFAULT_MESSAGES


class Column:
    def __init__(
        self,
        name: str,
        schema: Optional[gb.Schema] = None,
        *,
        required: Optional[bool] = None,
        predicate: Optional[Callable[[dict], bool]] = None,
        unique: bool = False,
    ):
        self.required = required
        self.name = name
        self.unique = unique
        self._schema = schema
        self._predicate = predicate

    def predicate_result(self, prev_values: dict):
        if self._predicate:
            return self._predicate(prev_values)
        else:
            return True

    def validate(self, value, typecast: bool = False, context: dict = {}):
        if self._schema:
            return self._schema(value, typecast=typecast, context=context)
        else:
            return value

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__

        return super().__eq__(other)


class ColumnBuilderError(Exception):
    pass


class ColumnBuilder:
    def __init__(self, column_schema_builder: ColumnSchemaBuilder):
        self._column_schema_builder = column_schema_builder

    def build(self, sa_column: sa.Column) -> Column:
        schema = self._column_schema_builder.build(sa_column)

        return Column(
            sa_column.name,
            schema,
            required=not sa_column.nullable,
            unique=sa_column.unique,
        )


column_builder = ColumnBuilder(column_schema_builder)


class MappedColumn:
    def __init__(
        self,
        sa_mapped_class: type,
        sa_column: sa.Column,
        sa_pk_column: sa.Column,
        column: Column,
        messages: gb.MessageCollectionType = DEFAULT_MESSAGES,
    ):
        self._sa_mapped_class = sa_mapped_class
        self._sa_column = sa_column
        self._sa_pk_column = sa_pk_column
        self._column = column
        self._messages = messages

    @property
    def name(self):
        return self._column.name

    @property
    def required(self):
        return self._column.required

    def predicate_result(self, prev_values: dict):
        return self._column.predicate_result(prev_values)

    def validate(
        self,
        value,
        typecast: bool,
        context: dict,
        session: sa_orm.Session,
        instance: Optional[Any] = None,
    ):
        value = self._column.validate(value, typecast, context)

        if self._column.unique:
            query = sa_orm.Query(self._sa_mapped_class).filter(self._sa_column == value)

            if instance:
                instance_pk = getattr(instance, self._sa_pk_column.name)
                query = query.filter(self._sa_pk_column != instance_pk)

            exists = session.query(query.exists()).scalar()

            if exists:
                raise gb.SchemaError([self._error("already_exists")])

        return value

    def _error(self, code: str, args: dict = {}, nested_errors: dict = {}):
        return gb.Error(code, args, nested_errors, self._messages.get_message(code))


class MappedColumnBuilderError(Exception):
    pass


class MappedColumnBuilder:
    def build(
        self,
        sa_mapped_class: type,
        column: Column,
        messages: gb.MessageCollectionType = DEFAULT_MESSAGES,
    ) -> Column:
        return MappedColumn(
            sa_mapped_class,
            self._get_sa_column(sa_mapped_class, column.name),
            self._get_pk_sa_column(sa_mapped_class),
            column,
            messages,
        )

    def _get_sa_column(self, sa_mapped_class: type, column_name: str) -> sa.Column:
        sa_mapper = sa.inspect(sa_mapped_class)

        if column_name not in sa_mapper.columns:
            raise MappedColumnBuilderError(
                f"mapped class {sa_mapped_class.__name__} has no column {column_name}"
            )

        return sa_mapper.columns[column_name]

    def _get_pk_sa_column(self, sa_mapped_class: type) -> sa.Column:
        sa_mapper = sa.inspect(sa_mapped_class)

        if len(sa_mapper.primary_key) > 1:
            raise MappedColumnBuilderError(
                "mapped classes with composite primary keys are not supported"
            )

        return sa_mapper.primary_key[0]


mapped_column_builder = MappedColumnBuilder()
