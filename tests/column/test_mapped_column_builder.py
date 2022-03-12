import goodboy as gb
import sqlalchemy as sa

from goodboy_sqlalchemy.column import Column, MappedColumnBuilder

Base = sa.orm.declarative_base()


class Dummy(Base):
    __tablename__ = "dummies"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)


def test_build_method():
    column = Column("name", gb.Str())

    builder = MappedColumnBuilder()
    builder.build(Dummy, Dummy.name, Dummy.id, column)
