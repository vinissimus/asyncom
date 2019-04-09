
"""Tests for `pgorm` package."""

import pytest
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
from sqlalchemy import orm

Base = declarative_base()


class OrmTest(Base):
    __tablename__ = 'orm_test'

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(100), index=True)
    value = sa.Column(sa.Text)

    # many = orm.relationship('ManyTests', lazy='noload')
    # we cannot support relationships
    # props = has_many('ManyTests')

    @property
    def items(self):
        qs = self.__db__.query(
            ManyTests).filter_by(id_orm=self.id)
        return qs

    async def append_item(self, value):
        await self.__db__.add(
            ManyTests(id_orm=self.id, other=value)
        )


class ManyTests(Base):
    __tablename__ = 'orm_manytest'

    id = sa.Column(sa.Integer, primary_key=True)
    id_orm = sa.Column(
        sa.ForeignKey('orm_test.id', match=u'FULL'),
    )
    other = sa.Column(sa.String(200))


@pytest.fixture
async def data(async_db):
    url = str(async_db.url)
    engine = sa.create_engine(url)
    Base.metadata.create_all(engine)



@pytest.mark.asyncio
async def test_fixture_is_working(async_db, data):
    res = await async_db.execute("SELECT 1")
    assert res == 1
    ins = OrmTest(name="test", value="xxxx")
    res = await async_db.add(ins)
    assert ins.id == 1
    res = await async_db.query(OrmTest).count()
    assert res == 1
    reg = await async_db.query(OrmTest).get(1)
    reg.name = "test2"
    await async_db.update(reg)
    reg2 = await async_db.query(OrmTest).get(1)
    assert reg2.name == "test2"
    await async_db.remove(reg2)
    assert 0 == await async_db.query(OrmTest).count()


@pytest.mark.asyncio
async def test_aiter(async_db, data):
    ins = OrmTest(name="test", value="xxxx")
    ins2 = OrmTest(name="test2", value="xxxx")
    await async_db.add(ins, ins2)
    assert await async_db.query(OrmTest).count() == 2
    count = 0
    for k in await async_db.query(OrmTest).all():
        count += 1
    assert count == 2

    assert await async_db.query(OrmTest).filter(
        OrmTest.name == "test").count() == 1

    await async_db.add(
        ManyTests(id_orm=ins.id, other='value 1'),
        ManyTests(id_orm=ins.id, other='value 2')
    )
    ins.__db__ = async_db
    assert await ins.items.count() == 2
    await ins.append_item('value 3')
    assert await ins.items.count() == 3

    res = await async_db.query(ManyTests).order_by(
        ManyTests.other.desc()).all()

    assert res[0].other == 'value 3'
    assert res[1].other == 'value 2'

    await ins.items.delete()
    assert await ins.items.count() == 0

    async def exists():
        return await async_db.query(sa.sql.exists().where(
            OrmTest.id == ins.id
        )).scalar()

    assert await exists() is True
    await async_db.delete(ins)
    assert await exists() is False




