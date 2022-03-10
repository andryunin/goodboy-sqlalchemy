import pytest
from goodboy import Key, Str

from goodboy_sqlalchemy.columns import Column


@pytest.mark.parametrize("required", [True, False])
def test_column_dict_key_method(required):
    column = Column("dummy", Str(), required=required)

    assert column.get_dict_key() == Key("dummy", Str(), required=required)
