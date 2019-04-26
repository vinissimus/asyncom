
"""Main module."""


from collections import Iterable

from sqlalchemy import inspect, sql
from sqlalchemy.orm import Query
from sqlalchemy.orm import exc as orm_exc

from databases import Database


class OMQuery(Query):
    def __init__(self, entities, database=None):
        self.__db = database
        self._all = None
        super().__init__(entities, session=None)

    async def all(self):
        context = self._compile_context()
        context.statement.use_labels = True
        return await self._execute(context)

    async def get(self, ident):
        mapper = self._only_full_mapper_zero("get")
        pk = mapper.primary_key
        return await self.filter(pk[0] == ident).one_or_none()

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

    def __aiter__(self):
        return self.iterate()

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

    async def iterate(self):
        context = self._compile_context()
        context.statement.use_labels = True
        fn = self.get_mapper(context)
        async for row in self.__db.iterate(context.statement):
            yield fn(row)

    async def _execute(self, context):
        result = await self.__db.fetch_all(context.statement)
        return self.map_to_instances(result, context)

    def get_mapper(self, context):
        entity = self._entity_zero()
        prefixes = get_prefixes(context.statement._columns_plus_names)

        def map_result(v):
            return entity.entity(
                **{prefixes[k]: v for k, v in dict(v).items()})
        return map_result

    def map_to_instances(self, result, context):
        fn = self.get_mapper(context)
        return [fn(r) for r in result]

    async def delete(self):
        context = self._compile_context()
        entity = self._entity_zero().entity
        op = sql.delete(entity.__table__, context.whereclause)
        return await self.__db.execute(op)


def get_prefixes(cols):
    res = {}
    for key, col in cols:
        name = []
        if col.table.schema:
            name.append(col.table.schema)
        name.append(col.table.name)
        prefix = "_".join(name)
        res[key] = key.replace(prefix + '_', "")
    return res


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
        return await insert(ins, self)

    async def update(self, ins):
        return await update(ins, self)

    async def remove(self, ins):
        mapper = inspect(ins).mapper
        pk_column = mapper.primary_key[0]
        pk_value = getattr(ins, pk_column.name)
        expr = ins.__table__.delete().where(
            pk_column == pk_value
        )
        ins = None
        return await self.execute(expr)

    delete = remove

    @property
    def raw(self):
        # pointer to the raw asyncpg connection
        return self.connection().raw_connection


class OMBase:
    @classmethod
    def select(cls, *args, **kwargs):
        return cls.__table__.select(*args, **kwargs)


async def insert(ins, conn):
    mapper = ins.__mapper__
    pk_val = None
    first = True
    for table in mapper.tables:
        values = {}
        for column in table.columns:
            val = getattr(ins, column.key)
            if val:
                values[column.name] = val
            elif column.default:
                if column.default.is_callable:
                    _val = column.default.arg({})
                    values[column.name] = _val
                    setattr(ins, column.name, _val)
                elif column.default.is_scalar:
                    values[column.name] = column.default.arg
                    setattr(ins, column.name, column.default.arg)
                else:
                    raise NotImplementedError(
                        'we have limited support for column defaults, '
                        'only scalar and callables are allowed'
                    )

        expr = table.insert().values(values)
        _pk_val = await conn.execute(expr)
        # is first pk on inheritance chain (first table) and is not provided
        if first and _pk_val:
            pk_val = _pk_val
            setattr(ins, mapper.primary_key[0].key, pk_val)
            first = None

        if mapper.inherit_condition is not None:
            if (
                mapper.inherit_condition.left ==
                mapper.primary_key[0]
            ):
                col_name = mapper.inherit_condition.right.name
                setattr(ins, col_name, pk_val)
    return pk_val


async def update(ins, conn):
    mapper = ins.__mapper__
    for table in mapper.tables:
        values = {}
        for column in table.columns:
            val = getattr(ins, column.key)
            if val:
                values[column.name] = val
            elif column.onupdate:
                if column.onupdate.is_callable:
                    _val = column.onupdate.arg({})
                    values[column.name] = _val
                    setattr(ins, column.name, _val)
                elif column.onupdate.is_scalar:
                    values[column.name] = column.onupdate.arg
                    setattr(ins, column.name, column.onupdate.arg)
        pk_ = list(table.primary_key.columns)[0]
        expr = table.update().values(values).where(
            pk_ == values[pk_.key]
        )
        await conn.execute(expr)
