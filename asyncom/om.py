
"""Main module."""


from collections import Iterable

from sqlalchemy import inspect, sql
from sqlalchemy.orm import Query
from sqlalchemy.orm import exc as orm_exc

from databases import Database


class OMQuery(Query):
    def __init__(self, entities, database=None):
        self.__db = database
        super().__init__(entities, session=None)

    async def all(self):
        context = self._compile_context()
        context.statement.use_labels = True
        return await self._execute(context)

    async def get(self, ident):
        mapper = self._only_full_mapper_zero("get")
        pk = mapper.primary_key
        return await self.filter(pk[0] == ident).one()

    async def one_or_none(self):
        ret = await self.all()
        length = len(ret)
        if length == 1:
            return ret[0]
        elif length == 0:
            return None
        else:
            raise orm_exc.MultipleResultsFound(
                "Multiple rows were found for one_or_none()"
            )

    async def one(self):
        try:
            ret = await self.one_or_none()
        except orm_exc.MultipleResultsFound:
            raise orm_exc.MultipleResultsFound(
                "Multiple rows were found for one()"
            )
        else:
            if ret is None:
                raise orm_exc.NoResultFound("No row was found for one()")
        return ret

    async def count(self):
        col = sql.func.count(sql.literal_column("*"))
        return await self.from_self(col).scalar()

    async def scalar(self):
        context = self._compile_context()
        context.statement.use_labels = True
        try:
            ret = await self.__db.fetch_val(context.statement)
            if not isinstance(ret, Iterable):
                return ret
            return ret[0]
        except orm_exc.NoResultFound:
            return None

    async def _execute(self, context):
        result = await self.__db.fetch_all(context.statement)
        return self.map_to_instances(result, context)

    def map_to_instances(self, result, context=None):
        entity = self._entity_zero()
        table = entity.entity.__tablename__ + "_"

        def map_result(v):
            res = {}
            for k, v in dict(v).items():
                res[k.split(table)[1]] = v
            return res
        return [entity.entity(**map_result(r)) for r in result]


class OMDatabase(Database):
    def query(self, args):
        return OMQuery(args, database=self)

    async def add(self, *args):
        res = []
        for ins in args:
            await self._add_impl(ins)
        if len(res) == 1:
            return res[0]
        return res

    async def _add_impl(self, ins):
        mapper = inspect(ins).mapper
        # currently only support for one primary key
        _pk = mapper.primary_key[0].name
        values = {
            c.key: getattr(ins, c.key)
            for c in mapper.column_attrs if c.key != _pk
        }
        expr = ins.__table__.insert().values(values)
        pk = await self.execute(expr)
        setattr(ins, _pk, pk)
        return pk

    async def update(self, ins):
        mapper = inspect(ins).mapper
        pk_column = mapper.primary_key[0]
        pk_value = getattr(ins, pk_column.name)
        values = {
            c.key: getattr(ins, c.key)
            for c in mapper.column_attrs if c.key != pk_column.name
        }
        expr = ins.__table__.update().values(values).where(
            pk_column == pk_value
        )
        return await self.execute(expr)

    async def remove(self, ins):
        mapper = inspect(ins).mapper
        pk_column = mapper.primary_key[0]
        pk_value = getattr(ins, pk_column.name)
        expr = ins.__table__.delete().where(
            pk_column == pk_value
        )
        ins = None
        return await self.execute(expr)
