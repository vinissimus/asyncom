import pytest
import enum
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

from asyncom import OMBase

Base = declarative_base(cls=OMBase)


pytestmark = pytest.mark.asyncio


class OrmJSON(Base):
    __tablename__ = "orm_json"
    key = sa.Column(sa.String(254), primary_key=True)
    value = sa.Column(JSONB)


class ColTypes(enum.Enum):
    typea = "a"
    typeb = "b"
    typec = "c"


class OrmEnum(Base):
    __tablename__ = "orm_enum"
    key = sa.Column(sa.String(254), primary_key=True)
    value = sa.Column(sa.Enum(ColTypes))


@pytest.fixture
async def data(async_db):
    url = str(async_db.url)
    engine = sa.create_engine(url)
    Base.metadata.create_all(engine)


async def test_json_model(async_db, data):
    obj = OrmJSON(
        key="prop", value={"a": 1, "b": 2, "c": 3, "k": "vinissimus"}
    )
    await async_db.add(obj)
    res = await async_db.query(OrmJSON).get("prop")
    assert res.value["a"] == 1

    def query(cmp):
        return async_db.query(OrmJSON).filter(cmp).one()

    el = await query(OrmJSON.value["a"].astext == "1")
    assert el.value["b"] == 2

    el = await query(OrmJSON.value.has_key("a"))  # noqa
    assert el.value["b"] == 2

    el = await query(OrmJSON.value.contains({"k": "vinissimus"}))
    assert el.value["b"] == 2


async def test_enum(async_db, data):
    en = OrmEnum(key="a", value=ColTypes.typea)
    await async_db.add(en)

    res = await async_db.query(OrmEnum).filter(
        OrmEnum.value == ColTypes.typea).one()
    assert res.key == "a"
