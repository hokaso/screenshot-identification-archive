import pymysql

class DatabaseHandler(object):
    def __init__(self, host, username, password, database, port):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.db = pymysql.connect(self.host, self.username, self.password, self.database, self.port, charset='utf8')
        self.cursor = self.db.cursor()

    # 增删改数据库操作
    def modify_DB(self, sql):
        try:
            self.cursor.execute(sql)
            # log = self.cursor.execute(sql)  # 返回 插入数据 条数 可以根据 返回值 判定处理结果
            # print(log)
            self.db.commit()
            return True
        except Exception as e:
            print(e)
            # 发生错误时回滚
            self.db.rollback()
            return False

    # 查数据库操作
    def search_DB(self, sql):
        try:
            self.cursor.execute(sql)
            # log = self.cursor.execute(sql) # 返回 查询数据 条数 可以根据 返回值 判定处理结果
            # print(log)
            data = self.cursor.fetchall() # 返回所有记录列表
        except Exception as e:
            print(e)
            data = None
        return data

    # 查数据库中某个表是否存在某个条目，存在返回具体值，不存在则返回None
    def exist_DB(self, table, entry, numerical):
        try:
            sql = "select * from "+ table +" where "+ entry +" = \""+ numerical +"\" LIMIT 1"
            self.cursor.execute(sql)
            col = self.cursor.description
            data = self.cursor.fetchone()  # 返回指定记录列表
        except Exception as e:
            print(e)
            data = None
            col = None
        return data, col

    # 当程序调用表中某个条目时，该条目的调用计数字段+1
    def count_DB(self, table, entry, numerical):
        try:
            sql = "update " + table + " set call_count=call_count+'1' where "+ entry +" = \""+ numerical +"\" LIMIT 1"
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()


if __name__ == '__main__':
    DbHandle = DataBaseHandler('10.92.6.176', 'root', 'root', 'devry', 3306)
    process_info, process_col = DbHandle.exist_DB("bot_config", "id", "LocalTest")
    process_dict = {}
    for k, i in enumerate(process_col):
        _process_dict = {"bot_"+i[0] : process_info[k]}
        process_dict = {**process_dict, **_process_dict}
    print(process_dict)