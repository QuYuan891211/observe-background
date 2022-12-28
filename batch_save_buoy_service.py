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
import redis
import configparser
import log


class ConfigInfo:
    def __init__(self, db, name_config, psw, user, port, url):
        # print('in init')
        self.db = db
        self.name_config = name_config
        self.psw = psw
        self.user = user
        self.url = url
        self.port = port


class ConfigInit():
    def __init__(self):
        self.config = configparser.ConfigParser()  # 实例化一个config对象
        self.file = "config.ini"
        self.config.read(self.file)

    def read_database_info(self, database_name):
        # 2. get()   读取section对应key的值
        db = self.config.get(database_name, 'db')
        name_config = self.config.get(database_name, 'name_config')
        psw = self.config.get(database_name, 'psw')
        user = self.config.get(database_name, 'user')
        port = self.config.get(database_name, 'port')
        url = self.config.get(database_name, 'url')
        config_info = ConfigInfo(db, name_config, psw, user, port, url)
        return config_info
        # print("get()获取的值：" + value)

    def read_data_path(self, name):
        return self.config.get('data', name)

    def read_log_path(self):
        return self.config.get('log', 'log_path')


# 读取观测数据
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


def df_to_sql(df, name, db, psw, user, ip):
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

    # 0.数据库相关信息
    config_init = ConfigInit()
    config_info_mysql = config_init.read_database_info('mysql')
    db = config_info_mysql.db
    name_config = config_info_mysql.name_config
    psw = config_info_mysql.psw
    user = config_info_mysql.user
    port = config_info_mysql.port
    url = config_info_mysql.url
    ip = url + ':' + port

    # TODO:部署前query_time_start和query_time_end修改为获得当前需要查询的时间
    query_time_start = datetime(2022, 11, 16, 16, 0, 0)
    query_time_end = datetime(2022, 11, 18, 8, 0, 0)
    query_time = query_time_start

    # 获取Redis存储记录
    config_info_redis = config_init.read_database_info('redis')
    host_redis = config_info_redis.url
    port_redis = config_info_redis.port
    db_redis = config_info_redis.db
    password_redis = config_info_redis.psw
    r = redis.StrictRedis(host_redis, port_redis, db_redis, password_redis, decode_responses=True)
    # if(dict.has_key(key)):
    record_list = r.lrange('buoy', 0, -1)
    print('当前已存数据时间： %s' % record_list)

    # 配置log文件
    logpath = config_init.read_log_path()
    print(logpath)
    # log = log.Logger(logpath, level='info')
    log = log.Logger(r'%s' % logpath, level='debug')

    while query_time <= query_time_end:
        # 查询时间转换为字符串
        query_time_str = query_time.strftime('%Y%m%d%H')
        query_year_str = query_time.strftime('%Y')
        query_mon_str = query_time.strftime('%m')
        # query_time_str = '2022111620'
        # query_time = time.strftime("%Y-%m-%d %H:%M:%S", query_time_str)

        # 1. 观测点数据路径
        file_path_root = config_init.read_data_path('data_path')
        file_path = r'%s\%s\%s\%s.csv' % (file_path_root, query_year_str, query_mon_str, query_time_str)
        # print(file_path)
        # 2. 写入观测点基础信息

        if (os.path.exists(file_path)):
            if (query_time_str in record_list):
                error_message.append('%s 已在记录中，本次不进行入库操作' % query_time_str)
            else:
                df = read_obs_config(file_path, query_time)

                # 3. 观测点基础信息写入数据库

                df_to_sql(df, name_config, db, psw, user, ip)

                r.lpush('buoy', query_time_str)
                end = time.process_time()
                print('%s 数据入库完成， 用时(秒): ' % query_time_str, end - start)
        else:
            error_message.append('%s文件不存在, 未入库' % file_path)
        query_time += dt.timedelta(hours=1)
    # 写入Log文件并在屏幕中输出
    info = '批处理程序执行完成'
    # print(info)
    log.logger.info(info)

    if len(error_message) > 0:
        # for item in error_message:
        #     print('错误信息：%s' % item)
        log.logger.error(error_message)

    # log.logger.info('==={}===\n'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
