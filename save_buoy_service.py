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
    df.rename(columns={'TIME': 'record_time', 'AT': 'att'}, inplace=True)

    # 处理时间
    times = time.time()
    local_time = time.localtime(times)
    formate_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    df.insert(loc=2, column='create_time', value=formate_time)
    df.insert(loc=3, column='modified', value=formate_time)
    df.insert(loc=0, column='query_time', value=query_time)
    # df.insert(loc=0, column)
    df = df.drop('LON', axis=1)
    df = df.drop('LAT', axis=1)
    # TODO:在质量控制完成后，删除此句
    df = df.replace('/', np.nan)
    # print(df)
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
    error_message_exits = []
    error_message_missing = []

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

    # 修改为制定日期的时间段，注意个位数月份和时间不要加0
    # query_time_start = datetime(2023, 1, 1, 8, 0, 0)
    # query_time_end = datetime(2023, 1, 6, 8, 0, 0)
    query_time = dt.datetime.now().replace(minute=0, second=0, microsecond=0)
    query_time_start = query_time
    query_time_end = query_time
   
	
    # 获取Redis存储记录
    config_info_redis = config_init.read_database_info('redis')
    host_redis = config_info_redis.url
    port_redis = config_info_redis.port
    db_redis = config_info_redis.db
    password_redis = config_info_redis.psw
    r = redis.StrictRedis(host_redis, port_redis, db_redis, password_redis, decode_responses=True)
    # if(dict.has_key(key)):
    record_list = r.lrange('buoy', 0, -1)
    # print('当前已存数据时间： %s' % record_list)

    # 配置log文件
    logpath = config_init.read_log_path()
    print('日志文件目录： ' + logpath)
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
                error_message_exits.append(query_time_str)
            else:
                df = read_obs_config(file_path, query_time)

                # 3. 观测点基础信息写入数据库

                df_to_sql(df, name_config, db, psw, user, ip)

                r.lpush('buoy', query_time_str)

                print('%s 数据入库完成 ' % query_time_str)
        else:
            error_message_missing.append(query_time_str)
        query_time += dt.timedelta(hours=1)
    # 写入Log文件并在屏幕中输出

    end = time.process_time()
    info = '程序执行完成, 总用时(秒)： %s' % (end - start)

    # print(info)
    log.logger.info(info)

    if len(error_message_exits) > 0:
        # for item in error_message:
        #     # print('错误信息：%s' % item)
        log.logger.error('以下CSV文件重复入库，本次不进行入库操作： ' + str(error_message_exits))
    if len(error_message_missing) > 0:
        # for item in error_message:
        #     # print('错误信息：%s' % item)
        log.logger.error('以下CSV文件在目标路径中不存在，请核对文件信息： ' + str(error_message_missing))
    # log.logger.info('==={}===\n'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
