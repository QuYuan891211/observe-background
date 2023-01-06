import datetime as dt
import time
from datetime import datetime
# TODO:部署前query_time_start和query_time_end修改为获得当前需要查询的时间
query_time_start = datetime(2022, 11, 16, 16, 0, 0)
query_time_end = datetime(2022, 12, 29, 11, 0, 0)
query_time = dt.datetime.now()
query_time_str = query_time.strftime('%Y%m%d%H')
query_year_str = query_time.strftime('%Y')
query_mon_str = query_time.strftime('%m')
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(query_time_str)
    print(query_year_str)
    print(query_mon_str)
