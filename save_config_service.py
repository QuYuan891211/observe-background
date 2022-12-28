# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# %%
from urllib import parse

import pandas as pd
from sqlalchemy import create_engine
import time


# 读取观测点基础信息表
def read_obs_config(file_path):
    # Use a breakpoint in the code line below to debug your script.
    # print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
    # a = 1
    df = pd.read_csv(file_path, encoding='gbk')
    # dropping the rows having NaN values
    df = df.dropna(axis=0, how='all')
    df = df.dropna(axis=1, how='all')
    times = time.time()
    local_time = time.localtime(times)
    formate_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    df.insert(loc=5, column='create', value=formate_time)
    df.insert(loc=6, column='modified', value=formate_time)
    print(df)
    return df


def df_to_sql(df, name, db, psw, user, ip='localhost:3306'):
    '''
    :param df: The data to pass
    :param name: The name of the table in db
    :param psw: Your password of your database
    :param ip: Your IP
    :param db: The name of your database
    :param user: root
    :return: None
    '''
    # 因为有@符号，sqlalchemy不支持，所以需要转义
    # pwd = parse.quote_plus(psw)
    con = create_engine(
        'mysql+pymysql://{}:{}@{}/{}'.format(user,psw,ip,db))  # mysql+pymysql的意思为：指定引擎为pymysql
    df.to_sql(name, con,if_exists='append',index=False)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start = time.process_time()
    # 0. 数据库相关信息
    db = ''
    name_config = ''
    psw = ''
    user = ''
    # 1. 观测点基础信息表路径

    file_path = r'F:\Projects\data\observe\test\obs_config.csv';
    # 2. 写入观测点基础信息
    df = read_obs_config(file_path)

    # 3. 观测点基础信息写入数据库

    df_to_sql(df, name_config, db, psw, user)
    end = time.process_time()
    print('time cost: ', end - start)
