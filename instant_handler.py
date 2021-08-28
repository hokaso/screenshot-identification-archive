from database_handler import DatabaseHandler

class InstantHandler(object):

    def __init__(self):
        # 初始化数据库
        self.db_handler = DatabaseHandler('localhost', 'root', '', 'ry-vue', 3306)

    def get_connect(self):
        return self.db_handler
