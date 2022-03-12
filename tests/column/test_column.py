import pytest
from goodboy import Error, Int, type_name

from goodboy_sqlalchemy.column import Column
from tests.conftest import assert_errors


def test_validate_by_schema():
    column = Column("dummy", Int())

    assert column.validate(3000) == 3000

    with assert_errors([Error("unexpected_type", {"expected_type": type_name("int")})]):
        column.validate("oops")


def test_validate_when_no_schema_specified():
    column = Column("dummy")

    assert column.validate(3000) == 3000
    assert column.validate("ok") == "ok"


@pytest.mark.parametrize("predicate_result", [True, False])
def test_predicate_running(predicate_result):
    column = Column("dummy", Int(), predicate=lambda prev_values: predicate_result)

    assert column.predicate_result({}) == predicate_result


def test_equality_check():
    column_1 = Column("dummy", Int(), required=True)
    column_2 = Column("dummy", Int(), required=True)

    assert column_1 == column_2
