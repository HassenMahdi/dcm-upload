from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy_utils import database_exists, create_database
import os

__TABLE__ = "collection_data"


def get_engine(db):
    engine = create_engine(
        os.environ['MYSQL_URI'] + db + '?auth_plugin=mysql_native_password',
        echo=False)
    if not database_exists(engine.url):
        create_database(engine.url)
    return engine



def sink_to_mysql(df, domain_id,bdd):
    tableNme = f"{domain_id}"
    engine = get_engine(bdd)
    # df = pd.DataFrame(
    #     {'NAME': ['User 1', 'User 2', 'User 3'], 'lastname': ['aaa', 'bbb', 'ccc'], 'Test': ['aaa', 'bbb', 'ccc']})
    if table_exist(engine, tableNme):
        add_new_columns(df, engine, tableNme,bdd)
        df.to_sql(name=tableNme, con=engine, if_exists='append', index=False)
    else:
        df.to_sql(name=tableNme, con=engine, if_exists='append', index=False)


def add_new_columns(df, engine, tableName, bdd):
    new_cols = list(df.columns)
    old_cols = []
    res = engine.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "' and TABLE_SCHEMA = '" + bdd + "'")
    for i in range(len(res.cursor._rows)):
        old_cols.append(res.cursor._rows[i][0])
    diff = diff_entre_two_list(old_cols, new_cols)
    if not len(diff):
        return
    else:
        for col in diff:
            engine.execute("alter table " + tableName + " add column " + col + " text")


def diff_entre_two_list(l1, l2):
    missing = []
    d = [x.lower() for x in l1]
    for m in l2:
        if m.lower() not in d:
            missing.append(m)
    return missing


def table_exist(engine, tableName):
    res = engine.execute("SHOW TABLES LIKE '" + tableName + "'")
    if res.rowcount == 0:
        return False
    return True
