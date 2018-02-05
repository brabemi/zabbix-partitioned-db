import configparser
import re
import time

import click
import psycopg2
from psycopg2 import sql

def check_database_cfg(cfg_db):
    db_params = [
        'host',
        'port',
        'dbname',
        'username',
        'password',
    ]
    valid = True
    for param in db_params:
        if param not in cfg_db:
            valid = False
            print('Unable to find {} in database section of config'.format(param))
    return valid

def create_db_conn(cfg_db):
    if not check_database_cfg(cfg_db):
        exit(1)
    return psycopg2.connect(
        host=cfg_db['host'],
        port=cfg_db['port'],
        dbname=cfg_db['dbname'],
        user=cfg_db['username'],
        password=cfg_db['password']
    )

def check_task_cfg(cfg_task, taskname):
    params = [
        'table_name',
        'period',
        'count',
        'part_name',
        'create_index',
        'unique_index',
        'index_postfix',
        'index_cols',
    ]
    valid = True
    for param in params:
        if param not in cfg_task:
            valid = False
            print('Unable to find {} in {} section of config'.format(param, taskname))
    return valid

def get_tables(part_name, db):
    curr = db.cursor()
    part_name_like = part_name + '%'
    curr.execute(
        "SELECT tablename FROM pg_tables where tablename like %s;",
        (part_name_like,)
    )
    return [e[0] for e in curr.fetchall()]

def get_max_no(tables, part_name):
    prefix_len = len(part_name)
    max_id = 0
    for t in tables:
        part_id = t[prefix_len:]
        if part_id not in ('lower', 'upper'):
            part_id = int(part_id)
            if part_id > max_id:
                max_id = part_id
    return max_id

def get_new_tables(min_no, max_no, part_name, period):
    new_tables = []
    for no in range(min_no, max_no + 1):
        new_tables.append({
            'name': '{}{}'.format(part_name, no),
            'low': no * period,
            'up': (no * period) + period
        })
    return new_tables

def get_new_indexes(tables, uniq, postfix, cols):
    indexes = []
    for t in tables:
        indexes.append({
            'name': t['name'] + postfix,
            'uniq': uniq,
            'table_name': t['name'],
            'cols': cols
        })
    return indexes

def task_sql(cfg_task, db, tables, indexes):
    if not tables:
        return

    parent_table = cfg_task['table_name']
    part_name = cfg_task['part_name']
    index_postfix = cfg_task['index_postfix']
    create_index = cfg_task.getboolean('create_index')
    curr = db.cursor()

    try:
        part_upper = part_name + 'upper'
        curr.execute(
            sql.SQL('ALTER TABLE {} DETACH PARTITION {};')
            .format(sql.Identifier(parent_table), sql.Identifier(part_upper))
        )
        print(curr.query)
        curr.execute(
            sql.SQL('ALTER TABLE {} RENAME TO {};')
            .format(sql.Identifier(part_upper), sql.Identifier(part_upper + '_old'))
        )
        print(curr.query)
        if create_index:
            curr.execute(
                sql.SQL('ALTER TABLE {} RENAME TO {};')
                .format(sql.Identifier(part_upper + index_postfix), sql.Identifier(part_upper + '_old' + index_postfix))
            )
            print(curr.query)

        for t in tables:
            curr.execute(
                sql.SQL('CREATE TABLE {} PARTITION OF {} FOR VALUES FROM (%s) TO (%s);')
                .format(sql.Identifier(t['name']), sql.Identifier(parent_table)),
                (t['low'], t['up'])
            )
            print(curr.query)

        for i in indexes:
            query = 'CREATE {}'.format('UNIQUE ' if i['uniq'] else '') + 'INDEX {} ON {} ({});'
            curr.execute(
                sql.SQL(query).format(
                    sql.Identifier(i['name']),
                    sql.Identifier(i['table_name']),
                    sql.SQL(', ').join(map(sql.Identifier, i['cols'])),
                )
            )
            print(curr.query)

        max_table = max(tables, key=lambda e: e['up'])
        curr.execute(
            sql.SQL('CREATE TABLE {} PARTITION OF {} FOR VALUES FROM (%s) TO (MAXVALUE);')
            .format(sql.Identifier(part_upper), sql.Identifier(parent_table)),
            (max_table['up'],)
        )
        print(curr.query)

        if create_index:
            unique_index = cfg_task.getboolean('unique_index')
            index_cols = re.split(r'\W+', cfg_task['index_cols'])
            query = 'CREATE {}'.format('UNIQUE ' if unique_index else '') + 'INDEX {} ON {} ({});'
            curr.execute(
                sql.SQL(query).format(
                    sql.Identifier(part_upper + index_postfix),
                    sql.Identifier(part_upper),
                    sql.SQL(', ').join(map(sql.Identifier, index_cols)),
                )
            )
            print(curr.query)

        curr.execute(
            sql.SQL('INSERT INTO {} SELECT * FROM {};')
            .format(sql.Identifier(parent_table), sql.Identifier(part_upper + '_old'))
        )
        print(curr.query)
        curr.execute(
            sql.SQL('DROP TABLE {};')
            .format(sql.Identifier(part_upper + '_old'))
        )
        print(curr.query)
        db.commit()
    except psycopg2.Error as e:
        # TODO process properly
        print(e)
        db.rollback()

def task(cfg_task, taskname, db):
    if not check_task_cfg(cfg_task, taskname):
        print('Unable to procces task {}'.format(taskname))
        return

    period = int(cfg_task['period'])
    count = int(cfg_task['count'])
    part_name = cfg_task['part_name']
    current_time = int(time.time())
    part_no = current_time // period

    # TODO check if tables exist lower, upper, one or more ID/NO
    tables = get_tables(part_name, db)
    max_no = get_max_no(tables, part_name)

    if max_no > part_no + count:
        print('Fail max_no: {} > part_no: {} + count: {}'.format(max_no, part_no, count))
        return

    new_tables = get_new_tables(max_no + 1, part_no + count, part_name, period)

    create_index = cfg_task.getboolean('create_index')
    new_indexes = []
    if create_index:
        unique_index = cfg_task.getboolean('unique_index')
        index_postfix = cfg_task['index_postfix']
        index_cols = re.split(r'\W+', cfg_task['index_cols'])
        new_indexes = get_new_indexes(new_tables, unique_index, index_postfix, index_cols)

    task_sql(cfg_task, db, new_tables, new_indexes)

@click.command()
@click.option(
    '--config',
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False,
        writable=False, readable=True, resolve_path=True
    ),
    help='path to config file',
)
def main(config):
    cfg = configparser.ConfigParser()
    cfg.read(config)
    if 'database' not in cfg:
        print('Unable to find database section in config')
        return 1
    db = create_db_conn(cfg['database'])
    for section in cfg.sections():
        if section != 'database':
            task(cfg[section], section, db)

if __name__ == '__main__':
    main()
