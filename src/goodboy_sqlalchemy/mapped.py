from __future__ import annotations

from typing import Optional

from goodboy import Dict, Schema
from sqlalchemy import Column as SAColumn
from sqlalchemy.orm import ColumnProperty, Session, class_mapper

from goodboy_sqlalchemy.columns import Column, column_builder


class MappedError(Exception):
    pass


class Mapped(Schema):
    # TODO: customize messages (uniqueness errors, etc.)
    def __init__(
        self,
        mapped_class: type,
        columns: Optional[list[Column]] = [],
        column_names: Optional[list[str]] = [],
    ):
        super().__init__()

        self._mapped_class = mapped_class
        self._pk_sa_column = self._get_pk_sa_column(mapped_class)
        self._columns = columns[:]

        if column_names:
            self._columns += self._build_columns_by_name(mapped_class, column_names)

        for column in self._columns:
            column.attach_mapped(mapped_class)

        dict_keys = [c.get_dict_key() for c in self._columns]
        self._dict_schema = Dict(keys=dict_keys)

    def __call__(self, value, *, typecast=False, context: dict = {}):
        if not isinstance(context.get("session"), Session):
            raise MappedError(
                "session instance is required in Mapped validation context"
            )

        instance = context.get("mapped_instance")

        if instance:
            context["mapped_primary_key"] = getattr(instance, self._pk_sa_column.name)
        else:
            context["mapped_primary_key"] = None

        context["mapped_pk_sa_column"] = self._pk_sa_column

        return self._dict_schema(value, typecast=typecast, context=context)

    # TODO: support composite primary keys
    def _get_pk_sa_column(self, mapped_class: type) -> SAColumn:
        primary_key = class_mapper(mapped_class).primary_key

        if len(primary_key) > 1:
            raise MappedError(
                "mapped classes with composite primary keys are not supported"
            )

        return primary_key[0]

    def _build_columns_by_name(self, mapped_class: type, column_names: list[str]):
        columns = []

        for prop in class_mapper(mapped_class).iterate_properties:
            if not isinstance(prop, ColumnProperty):
                continue

            if prop.key not in column_names:
                continue

            if len(prop.columns) > 1:
                continue

            columns.append(column_builder.build(mapped_class, prop.columns[0]))

        return columns
