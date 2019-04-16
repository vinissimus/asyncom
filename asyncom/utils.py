
import sys
from os.path import join
from os.path import dirname

import yaml


async def import_data(sess, file, base):
    """ loads data from a fixture file
        format:
        - model: table_name
          data:
          - id: 1
            prop: value
          - id: 2
            prop: value
    """
    if hasattr(file, "read"):
        data = file.read()
    else:
        data = file

    rows = yaml.load(data, Loader=yaml.UnsafeLoader)
    if isinstance(rows, str):
        data = load_file(file)
        rows = yaml.load(data, Loader=yaml.UnsafeLoader)

    for model in rows:
        Table = base.metadata.tables.get(model['model'])
        for row in model['data']:
            stm = Table.insert().values(row)
            await sess.execute(stm)


def load_file(file):
    try:
        with open(file, "r") as r:
            return r.read()
    except FileNotFoundError:
        _prefix = sys._getframe(2).f_globals.get('__file__')
        with open(join(dirname(_prefix), file), 'r') as f:
            return f.read()
