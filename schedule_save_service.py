# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# %%
from urllib import parse
import os
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
import time
from datetime import datetime
import numpy as np


# 读取观测点基础信息表
def read_obs_config(file_path, query_time):
    # Use a breakpoint in the code line below to debug your script.
    # print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
    # a = 1
    df = pd.read_csv(file_path, encoding='gbk')
    # dropping the rows having NaN values
    df = df.dropna(axis=0, how='all')
    df = df.dropna(axis=1, how='all')
    # 处理时间
    times = time.time()
    local_time = time.localtime(times)
    formate_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    df.insert(loc=2, column='create', value=formate_time)
    df.insert(loc=3, column='modified', value=formate_time)
    df.insert(loc=0, column='query_time', value=query_time)
    # df.insert(loc=0, column)
    df = df.drop('LON', axis=1)
    df = df.drop('LAT', axis=1)
    # TODO:在质量控制完成后，删除此句
    df = df.replace('/', np.nan)

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
        'mysql+pymysql://{}:{}@{}/{}'.format(user, psw, ip, db))  # mysql+pymysql的意思为：指定引擎为pymysql
    df.to_sql(name, con, if_exists='append', index=False)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start = time.process_time()
    error_message = []
    # TODO:部署前修改为获得当前需要查询的时间
    query_time_start = datetime(2022, 11, 16, 16, 0, 0)
    query_time_end = datetime(2022, 11, 18, 8, 0, 0)
    query_time = query_time_start

    #TODO: 部署前取消循环
    while query_time <= query_time_end:
        # 查询时间转换为字符串
        query_time_str = query_time.strftime('%Y%m%d%H')
        query_year_str = query_time.strftime('%Y')
        query_mon_str = query_time.strftime('%m')
        # query_time_str = '2022111620'
        # query_time = time.strftime("%Y-%m-%d %H:%M:%S", query_time_str)

        # 0. 数据库相关信息
        db = ''
        name_config = ''
        psw = ''
        user = ''
        # 1. 观测点基础信息表路径
        file_path = r'F:\Projects\data\observe\test\%s\%s\%s.csv' % (query_year_str, query_mon_str, query_time_str)
        print(file_path)
        # 2. 写入观测点基础信息
        if(os.path.exists(file_path)):
            df = read_obs_config(file_path, query_time)

            # 3. 观测点基础信息写入数据库

            df_to_sql(df, name_config, db, psw, user)
            end = time.process_time()
            print('%s 数据入库完成， 用时(秒): ' % query_time_str, end - start)
        else:
            error_message.append('%s文件不存在' % file_path)
        query_time += dt.timedelta(hours=1)
    print('全部数据入库完成')
    if len(error_message) > 0:
        # print(len(error_message))
        for item in error_message:
            print('错误信息：%s，未入库' % item)
