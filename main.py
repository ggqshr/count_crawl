import pymongo
import calendar
import datetime
from tqdm import tqdm

DB_COLLECT_FILE = "servers"


def get_database_and_collect():
    """
    从文件中读取数据库名和集合名以及他们的地址
    :return:
    """
    all_data_dict = {}
    with open(DB_COLLECT_FILE, 'r') as f:
        for line in f.readlines():
            data_list = line.split(":")
            assert len(data_list) == 3
            db_name, col_name = data_list[0].split(".")  # 获得数据库名和集合名
            ip_addr, port = data_list[1], int(data_list[2])
            all_data_dict[db_name] = (col_name, ip_addr, port)
    return all_data_dict


def connect2db(db_name, col_name, ip, port):
    """
    按照给定的连接和数据库名连接数据库并查询将结果返回
    :param db_name:
    :param col_name:
    :param ip:
    :param port:
    :return:
    """
    client = pymongo.MongoClient(ip, port)
    collect = client.get_database(db_name).get_collection(col_name)
    group = {}
    group['_id'] = "$post_time"
    group['count'] = {
        "$sum": 1
    }
    ss = collect.aggregate([{"$group": group}])
    return ss


def sort_fill_dict(all_date):
    """
    将所有数据中出现过的日期进行排序并填充一个从最小日期到最大日期为key，0为value的字典
    :param all_date:
    :return:
    """
    all_date = list(filter(lambda x: len(x) >= 10, all_date))  # 过滤掉字符串不是标准日期的结果
    all_date.sort()
    min_date = all_date[0]
    max_date = all_date[len(all_date) - 1]
    range_date_dict = generate_date_dict(min_date, max_date)
    return range_date_dict


def getEveryDay(begin_date, end_date):
    """
    获取两个日期之间的所有日期
    :param begin_date:
    :param end_date:
    :return:
    """
    date_list = []
    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)
    return date_list


def generate_date_dict(min_date, max_date):
    """
    根据两个日期获取他们之间的所有日期组成的字典，且所有的value为0
    :param min_date:
    :param max_date:
    :return:
    """
    range_date = getEveryDay(min_date, max_date)
    range_date_dict = {dd: 0 for dd in range_date}
    return range_date_dict


def write2file(db_name, col_name, date_dict: dict) -> None:
    """
    将结果写入文件
    :param date_dict:
    :return:
    """
    with open(f"{datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')}{db_name}.{col_name}.csv", "w",
              encoding="utf-8") as f:
        total_count = 0
        f.write("date,count\n")
        for item in tqdm(date_dict.items()):
            f.write(",".join([str(i) for i in item]) + "\n")
            total_count += item[1]
        f.write(f"total,{total_count}\n")


if __name__ == '__main__':
    # 获取所有连接信息
    all_data_dict = get_database_and_collect()
    # 连接数据库并按照日期进行聚合
    result = connect2db("YJS", *all_data_dict['YJS'])
    # 将结果转为字典，日期为key，对应日期的数量为value
    all_date = {item['_id']: item["count"] for item in result}
    # 排序并过滤，生成最大日期和最小日期之间的所有日期填充的字典
    range_date_dict = sort_fill_dict(all_date.keys())
    # 根据查询的结果更新字典
    range_date_dict.update(all_date)
    write2file("YJS", all_data_dict['YJS'][0], range_date_dict)
