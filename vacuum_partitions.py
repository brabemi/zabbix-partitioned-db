import configparser
import re
import time

import click
import psycopg2
from psycopg2 import sql

from pprint import pprint

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
        'part_name',
        'vacuum_period',
        'vacuum_window',
        'vacuum_delay',
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

def task(cfg_task, taskname, db):
    if not check_task_cfg(cfg_task, taskname):
        print('Unable to procces task {}'.format(taskname))
        return

    period = int(cfg_task['period'])
    part_name = cfg_task['part_name']
    vacuum_delay = int(cfg_task['vacuum_delay'])
    vacuum_period = list(map(int, re.split(r'\W+', cfg_task['vacuum_period'])))
    vacuum_window = list(map(int, re.split(r'\W+', cfg_task['vacuum_window'])))

    current_time = int(time.time())
    offseted_time = current_time - vacuum_delay
    base_part_no = offseted_time // period
    time_window_min = base_part_no * period + vacuum_window[0]
    time_window_max = base_part_no * period + vacuum_window[1]

    if offseted_time < time_window_min or offseted_time > time_window_max:
        return

    tables = get_tables(part_name, db)
    nos = set(get_nos(tables, part_name))
    db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    curr = db.cursor()
    for period in vacuum_period:
        no = base_part_no - 1 - period
        if no in nos:
            try:
                name = '{}{}'.format(part_name, no)
                print('Executing: VACUUM FULL ANALYZE {};'.format(sql.Identifier(name)))
                curr.execute(
                    sql.SQL('VACUUM FULL ANALYZE {};')
                   .format(sql.Identifier(name))
                )
            except psycopg2.Error as e:
                print(e)

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
