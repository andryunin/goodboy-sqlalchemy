from __future__ import annotations

from typing import Any, Optional

from goodboy import Error, Schema, SchemaError, SchemaErrorMixin, SchemaRulesMixin
from goodboy.messages import (
    DEFAULT_MESSAGES,
    MessageCollection,
    MessageCollectionType,
    type_name,
)
from sqlalchemy import Column as SAColumn
from sqlalchemy import inspect
from sqlalchemy.orm import ColumnProperty, Query, Session, class_mapper

from goodboy_sqlalchemy.columns import Column, ColumnBuilder, column_builder


class MappedColumn:
    def __init__(
        self,
        sa_mapped_class: type,
        sa_column: SAColumn,
        sa_pk_column: SAColumn,
        column: Column,
        messages: MessageCollection,
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

    def validate(self, value, typecast: bool, context: dict, instance: Optional[Any]):
        try:
            value = self._column.validate(value, typecast, context)
        except SchemaError as e:
            return None, e.errors

        if self._column.unique:
            query = Query(self._sa_mapped_class).filter(self._sa_column == value)

            if instance:
                instance_pk = getattr(instance, self._sa_pk_column.name)
                query = query.filter(self._sa_pk_column != instance_pk)

            exists = context["session"].query(query.exists()).scalar()

            if exists:
                return value, [self._error("already_exists")]

        return value, []

    def _error(self, code: str, args: dict = {}, nested_errors: dict = {}):
        return Error(code, args, nested_errors, self._messages.get_message(code))


class MappedError(Exception):
    pass


class Mapped(Schema, SchemaErrorMixin):
    def __init__(
        self,
        sa_mapped_class: type,
        columns: Optional[list[Column]] = [],
        column_names: Optional[list[str]] = [],
        column_builder: ColumnBuilder = column_builder,
        messages: MessageCollectionType = DEFAULT_MESSAGES,
    ):
        super().__init__()

        self._sa_mapped_class = sa_mapped_class
        self._pk_sa_column = self._get_pk_sa_column(sa_mapped_class)
        self._messages = messages

        self._columns = self._build_mapped_columns(
            sa_mapped_class, self._pk_sa_column, columns, messages
        )

        if column_names:
            self._columns += self._build_mapped_columns_by_name(
                sa_mapped_class,
                self._pk_sa_column,
                column_names,
                column_builder,
                messages,
            )

    def __call__(self, value, *, typecast=False, context: dict = {}):
        if not isinstance(context.get("session"), Session):
            raise MappedError(
                "session instance is required in Mapped validation context"
            )

        if not isinstance(value, dict):
            return None, [
                self._error("unexpected_type", {"expected_type": type_name("dict")})
            ]

        value, errors = self._validate(value, typecast, context)

        if errors:
            raise SchemaError(errors)

        return value

    # TODO: support composite primary keys
    def _get_pk_sa_column(self, sa_mapped_class: type) -> SAColumn:
        primary_key = class_mapper(sa_mapped_class).primary_key

        if len(primary_key) > 1:
            raise MappedError(
                "mapped classes with composite primary keys are not supported"
            )

        return primary_key[0]

    def _validate(self, value: dict, typecast: bool, context: dict):
        result: dict = {}

        key_errors = {}
        value_errors = {}

        unknown_columns = list(value.keys())

        instance = context.get("mapped_instance")

        for mapped_column in self._columns:
            if not mapped_column.predicate_result(result):
                continue

            if mapped_column.name in unknown_columns:
                unknown_columns.remove(mapped_column.name)

                column_value, column_errors = mapped_column.validate(
                    value[mapped_column.name], typecast, context, instance
                )

                if column_errors:
                    value_errors[mapped_column.name] = column_errors
                else:
                    result[mapped_column.name] = column_value
            else:
                if mapped_column.required:
                    key_errors[mapped_column.name] = [self._error("required_column")]

        errors: list[Error] = []

        if key_errors:
            errors.append(self._error("key_errors", nested_errors=key_errors))

        if value_errors:
            errors.append(self._error("value_errors", nested_errors=value_errors))

        return result, errors

    def _build_mapped_columns(
        self,
        sa_mapped_class: type,
        sa_pk_column: SAColumn,
        columns: list[Column],
        messages: MessageCollectionType,
    ) -> list[MappedColumn]:
        sa_mapper = inspect(sa_mapped_class)
        result: list[MappedColumn] = []

        for column in columns:
            try:
                sa_column = sa_mapper.columns[column.name]
            except KeyError:
                raise MappedError(
                    f"mapped class {sa_mapped_class.__name__} has no column {column.name}"
                )

            result.append(
                MappedColumn(sa_mapped_class, sa_column, sa_pk_column, column, messages)
            )

        return result

    def _build_mapped_columns_by_name(
        self,
        sa_mapped_class: type,
        sa_pk_column: SAColumn,
        column_names: list[str],
        column_builder: ColumnBuilder,
        messages: MessageCollectionType,
    ) -> list[MappedColumn]:
        result: list[MappedColumn] = []

        for prop in class_mapper(sa_mapped_class).iterate_properties:
            if not isinstance(prop, ColumnProperty):
                continue

            if prop.key not in column_names:
                continue

            if len(prop.columns) > 1:
                continue

            if prop.columns[0].name not in column_names:
                continue

            sa_column = prop.columns[0]
            column = column_builder.build(prop.columns[0])

            result.append(
                MappedColumn(sa_mapped_class, sa_column, sa_pk_column, column, messages)
            )

        return result
