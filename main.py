import pymongo
import calendar

DB_COLLECT_FILE = "servers"


def get_database_and_collect():
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


if __name__ == '__main__':
    all_data_dict = get_database_and_collect()
    connect2db("YJS", *all_data_dict['YJS'])
