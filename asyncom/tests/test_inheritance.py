
import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from asyncom import OMBase

Base = declarative_base(cls=OMBase)


class Element(Base):
    __tablename__ = 'element'

    id = sa.Column(sa.Integer, primary_key=True)
    key = sa.Column(sa.String(100), index=True)
    val = sa.Column(sa.Text)


class Published(Element):
    __tablename__ = 'element_published'
    element_id = sa.Column(
        sa.ForeignKey(
            'element.id', deferrable=True, initially=u'DEFERRED'
        ),
        primary_key=True
    )
    date = sa.Column(sa.DateTime)


class Abstract(Base):
    __abstract__ = True
    id = sa.Column(sa.Integer, primary_key=True)
    key = sa.Column(sa.String(100), index=True)
    val = sa.Column(sa.Text)


class Concrete(Abstract):
    __tablename__ = 'concrete'


@pytest.fixture
async def db(async_db):
    url = str(async_db.url)
    engine = sa.create_engine(url)
    Base.metadata.create_all(engine)
    yield async_db


@pytest.mark.asyncio
async def test_can_query_inherited_props(db):
    data = datetime.datetime.now()
    ele = Published(key='a', val='1', date=data)
    await db.add(ele)
    inherit = await db.query(Published).get(ele.id)
    assert inherit.key == 'a'
    assert inherit.val == '1'
    assert inherit.date == data
    inherit.key = 'b'
    await db.update(inherit)
    n = await db.query(Published).get(ele.id)
    assert n.key == 'b'


@pytest.mark.asyncio
async def test_operate_with_abstract_class(db):
    conc = Concrete(key='a', val='a')
    await db.add(conc)
    res = await db.query(Concrete).get(conc.id)
    assert res.id == conc.id
    assert res.key == conc.key
    res.key = 'b'
    await db.update(res)
    r2 = await db.query(Concrete).get(conc.id)
    assert r2.key == 'b'
