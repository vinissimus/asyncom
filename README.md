
# AsyncOM

Async OM it's a super basic Object mapper based almost all on sqlalchemy
ORM layer.

We use the declarative extension for building the classes, and also,
factor instances of them on querys. (*Limited support)

There is still no support for relations.

Depens on [encode/databases](https://github.com/encode/databases) dependency.


## Motivation

I don't like the asyncpgsa approach where they are just using,
the core layer, to build run the sqlgenerator. I like to build,
around the declarative layer of sqlachemy, and later found that
I can patch some of the methods on the session.query, to
use it's own sql generator, and turn async the query system.

Object persistent is minimal, and needs some love.


## Usage

```python

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
from databases import DatabaseURL

Base = declarative_base()


class OrmTest(Base):
    __tablename__ = 'orm_test'

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(100), index=True)
    value = sa.Column(sa.Text)

# Instead of usign Database from databases, you can use:
db = OMDatabase(DatabaseURL('postgres://root@postgres:{port}/guillotina'))

# instances of the object can be created with:
test = OrmTest(name='xx', value='yy')
await db.add(test)

print(test.id)  # pk column is correct set

# get an instance
ins = await db.query(OrmTest).get(1)

total = await db.query(OrmTest).count()
assert total == 1

# remove it
await db.delete(ins)

# filter them
res = await db.query(OrmTest).filter(
    OrmTest.name.like('xx')).all()

# Or just iterate over the results with a cursor:
async for row in db.query(OrmTest).filter(OrmTest.name.like('xx')):
    print(f'Row {row.name}: {row.value}')


# There is basic support for table inheritance query OneToOne


# Look at tests
```


