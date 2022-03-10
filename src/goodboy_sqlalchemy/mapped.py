from __future__ import annotations

from typing import Optional

from goodboy import Dict, Schema
from sqlalchemy.orm import ColumnProperty, Session, class_mapper

from goodboy_sqlalchemy.columns import Column, column_builder


class MappedContextError(Exception):
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
        self._columns = columns[:]

        if column_names:
            self._columns += self._build_columns_by_name(mapped_class, column_names)

        keys = [c.get_dict_key() for c in self._columns]

        self._dict_schema = Dict(keys=keys)

    def __call__(self, value, *, typecast=False, context: dict = {}):
        if not isinstance(context.get("session"), Session):
            raise MappedContextError(
                "session instance is required in Mapped validation context"
            )

        return self._dict_schema(value)

    def _build_columns_by_name(self, mapped_class: type, column_names: list[str]):
        columns = []

        for prop in class_mapper(mapped_class).iterate_properties:
            if not isinstance(prop, ColumnProperty):
                continue

            if prop.key not in column_names:
                continue

            if len(prop.columns) > 1:
                continue

            columns.append(column_builder.build(prop.columns[0]))

        return columns
