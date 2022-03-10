from __future__ import annotations

from typing import Optional

from goodboy import Dict, Schema
from sqlalchemy.orm import Session

from goodboy_sqlalchemy.columns import Column


class MappedContextError(Exception):
    pass


class Mapped(Schema):
    # TODO: customize messages (uniqueness errors, etc.)
    def __init__(
        self,
        mapped_class: type,
        columns: Optional[list[Column]] = [],
    ):
        super().__init__()

        self._mapped_class = mapped_class
        self._columns = columns

        keys = [c.get_dict_key() for c in columns]

        self._dict_schema = Dict(keys=keys)

    def __call__(self, value, *, typecast=False, context: dict = {}):
        if not isinstance(context.get("session"), Session):
            raise MappedContextError(
                "session instance is required in Mapped validation context"
            )

        return self._dict_schema(value)
