import configparser
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
        'history_count',
        'part_name',
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

def get_nos(tables, part_name):
    prefix_len = len(part_name)
    nos = []
    for t in tables:
        part_id = t[prefix_len:]
        if part_id not in ('lower', 'upper'):
            nos.append(int(part_id))
    return sorted(nos)

def task_sql(cfg_task, db, tables, upper_val):
    if not tables:
        return

    parent_table = cfg_task['table_name']
    part_name = cfg_task['part_name']
    curr = db.cursor()

    try:
        part_lower = part_name + 'lower'

        curr.execute(
            sql.SQL('ALTER TABLE {} DETACH PARTITION {};')
            .format(sql.Identifier(parent_table), sql.Identifier(part_lower))
        )
        print(curr.query)

        for t in tables:
            curr.execute(
                sql.SQL('ALTER TABLE {} DETACH PARTITION {};')
                .format(sql.Identifier(parent_table), sql.Identifier(t))
            )
            print(curr.query)

        curr.execute(
            sql.SQL('ALTER TABLE {} ATTACH PARTITION {} FOR VALUES FROM (MINVALUE) TO (%s);')
            .format(sql.Identifier(parent_table), sql.Identifier(part_lower)),
            (upper_val,)
        )
        print(curr.query)

        for t in tables:
            curr.execute(
                sql.SQL('INSERT INTO {} SELECT * FROM {};')
                .format(sql.Identifier(parent_table), sql.Identifier(t))
            )
            print(curr.query)

        for t in tables:
            curr.execute(
                sql.SQL('DROP TABLE {};')
                .format(sql.Identifier(t))
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
    history_count = int(cfg_task['history_count'])
    part_name = cfg_task['part_name']
    current_time = int(time.time())
    part_no = current_time // period

    # TODO check if tables exist lower, upper, one or more ID/NO
    tables = get_tables(part_name, db)
    nos = get_nos(tables, part_name)

    nos_for_consolidate = list(filter(lambda e: e + history_count < part_no, nos))
    if nos_for_consolidate:
        tables_for_consolidate = ['{}{}'.format(part_name, no) for no in nos_for_consolidate]
        max_consolidate_no = max(nos_for_consolidate)
        upper_val = (max_consolidate_no * period) + period
        task_sql(cfg_task, db, tables_for_consolidate, upper_val)

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
