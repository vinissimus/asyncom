
from collections import Iterable
from collections.abc import Mapping
from sqlalchemy import inspect
from sqlalchemy import sql
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import pypostgresql
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import Query
from sqlalchemy.sql.schema import Column

import asyncpg
import logging
import typing

logger = logging.getLogger("asyncom")

_result_processors = {}


def default_mapper_factory(query, context):
    entity = query._entity_zero()
    prefixes = get_prefixes(context.statement._columns_plus_names)

    def map_result(v):
        return entity.entity(
            **{prefixes[k]: v for k, v in dict(v).items()})
    return map_result


class Record(Mapping):
    def __init__(self, row, result_columns, dialect):
        self._row = row
        self._result_columns = result_columns
        self._dialect = dialect
        self._column_map = (
            {}
        )  # type: typing.Mapping[str, typing.Tuple[int, TypeEngine]]
        self._column_map_int = (
            {}
        )  # type: typing.Mapping[int, typing.Tuple[int, TypeEngine]]
        self._column_map_full = (
            {}
        )  # type: typing.Mapping[str, typing.Tuple[int, TypeEngine]]
        for idx, (column_name, _, column, datatype) in enumerate(
            self._result_columns
        ):
            self._column_map[column_name] = (idx, datatype)
            self._column_map_int[idx] = (idx, datatype)
            self._column_map_full[str(column[0])] = (idx, datatype)

    def __getitem__(self, key: typing.Any) -> typing.Any:
        if len(self._column_map) == 0:  # raw query
            return self._row[tuple(self._row.keys()).index(key)]
        elif type(key) is Column:
            idx, datatype = self._column_map_full[str(key)]
        elif type(key) is int:
            idx, datatype = self._column_map_int[key]
        else:
            idx, datatype = self._column_map[key]
        raw = self._row[idx]
        try:
            processor = _result_processors[datatype]
        except KeyError:
            processor = datatype.result_processor(self._dialect, None)
            _result_processors[datatype] = processor

        if processor is not None:
            return processor(raw)
        return raw

    def __iter__(self) -> typing.Iterator:
        return iter(self._row.keys())

    def __len__(self) -> int:
        return len(self._row)


class OMQuery(Query):
    def __init__(self, entities, database=None,
                 mapper_factory=default_mapper_factory):
        self.__db = database
        self._all = None
        self._mapper_factory = mapper_factory
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
        return self._mapper_factory(self, context)

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


class OMAsyncPG:
    def __init__(self, conn=None, url=None):
        self._con = conn
        self.url = url
        self._dialect = self._get_dialect()

    @property
    def connection(self):
        return self._con

    @property
    def dialect(self):
        return self._dialect

    async def connect(self):
        if not self._con:
            self._con = await asyncpg.connect(self.url)

    async def disconnect(self):
        if self.connection:
            await self.connection.close()
            self._con = None

    async def fetch_all(self, query, values=None):
        query, args, result_columns = self._compile(query)
        rows = await self.connection.fetch(query, *args)
        return [
            Record(row, result_columns, self._dialect) for row in rows
        ]

    async def fetch_one(self, query):
        query, args, result_columns = self._compile(query)
        row = await self.connection.fetchrow(query, *args)
        return Record(row, result_columns, self._dialect)

    async def fetch_val(self, query):
        query, args, result_columns = self._compile(query)
        return await self.connection.fetchval(query, *args)

    async def execute(self, query) -> typing.Any:
        query, args, result_columns = self._compile(query)
        return await self.connection.fetchval(query, *args)

    async def __aenter__(self):
        return self

    def transaction(self):
        return self.connection.transaction()

    async def execute_many(self, queries) -> None:
        # asyncpg uses prepared statements under the hood, so we just
        # loop through multiple executes here, which should all end up
        # using the same prepared statement.
        for single_query in queries:
            single_query, args, result_columns = self._compile(single_query)
            await self.connection.execute(single_query, *args)

    async def iterate(self, query):
        query, args, result_columns = self._compile(query)
        async for row in self.connection.cursor(query, *args):
            yield row

    def _get_dialect(self) -> Dialect:
        dialect = pypostgresql.dialect(paramstyle="pyformat")
        dialect.implicit_returning = True
        dialect.supports_native_enum = True
        dialect.supports_smallserial = True  # 9.2+
        dialect._backslash_escapes = False
        dialect.supports_sane_multi_rowcount = True  # psycopg 2.0.9+
        dialect._has_native_hstore = True
        dialect.supports_native_decimal = True
        return dialect

    def _compile(self, query):
        if isinstance(query, str):
            query = text(query)

        compiled = query.compile(dialect=self._dialect)
        compiled_params = sorted(compiled.params.items())

        mapping = {
            key: "$" + str(i) for i, (key, _) in enumerate(
                compiled_params, start=1
            )
        }
        compiled_query = compiled.string % mapping

        processors = compiled._bind_processors
        args = [
            processors[key](val) if key in processors else val
            for key, val in compiled_params
        ]

        query_message = compiled_query.replace(" \n", " ").replace("\n", " ")
        logger.debug(
            "Query: %s Args: %s",
            query_message,
            repr(tuple(args))
        )
        return compiled_query, args, compiled._result_columns


class OMDatabase(OMAsyncPG):

    def query(self, args, mapper_factory=default_mapper_factory):
        return OMQuery(args, database=self,
                       mapper_factory=mapper_factory)

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
        expr = pk_column.table.delete().where(
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


_marker = object()


async def insert(ins, conn):
    mapper = ins.__mapper__
    pk_val = None
    first = True
    for table in mapper.tables:
        values = {}
        for column in table.columns:
            val = getattr(ins, column.key)
            if val is not None:
                values[column.name] = val
            elif column.default:
                if column.default.is_callable:
                    _val = column.default.arg({})
                    values[column.name] = _val
                    setattr(ins, column.name, _val)
                elif column.default.is_scalar:
                    values[column.name] = column.default.arg
                    setattr(ins, column.name, column.default.arg)

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
            val = getattr(ins, column.key, _marker)
            if val != _marker:
                values[column.name] = val

            if column.onupdate:
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
