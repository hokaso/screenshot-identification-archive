import os, json, hashlib, shutil, pymysql, traceback
from instant_handler import InstantHandler
from snow_id import SnowId
from aip import AipOcr
from pic_resize import PicResize
from multiprocessing.pool import Pool
from multiprocessing import Lock, Manager
from functools import partial

ENCODING = 'utf-8'

source_path = "process/"
output_path = "complete/"
error_pth = "./error/"

BAIDU_APP_ID = ''
BAIDU_API_KEY = ''
BAIDU_SECRET_KEY = ''

image = None
db_handle = InstantHandler().get_connect()
client = AipOcr(BAIDU_APP_ID, BAIDU_API_KEY, BAIDU_SECRET_KEY)
pic = PicResize(4000)

class PushInto(object):

    # def __init__(self):
    #     self.image = None
    #     self.db_handle = InstantHandler().get_connect()
    #     self.client = AipOcr(BAIDU_APP_ID, BAIDU_API_KEY, BAIDU_SECRET_KEY)
    #     self.pic = PicResize(4000)

    def body(self, lock, ikey):
        # 重命名
        lock.acquire()
        new_name, old_name, reg = self.rename_mode(ikey)
        lock.release()
        _file_has_ext = source_path + new_name
        _file = source_path + reg
        _file_plus = _file + '.jpg'

        # 此处加入try-except循环
        # 讀取圖片文件
        # 返回图片大小
        try:
            pic_length = pic.resize(_file, _file_has_ext)
            if not pic_length:
                self.error(_file_has_ext)
                return None
        except Exception as e:
            print("图片转换问题：")
            print(e)
            traceback.print_exc()
            return None

        # 获取图片md5值
        with open(_file_plus, 'rb') as infile:
            new_image = infile.read()

        use_md5 = hashlib.md5()
        use_md5.update(new_image)
        pic_md5 = use_md5.hexdigest()

        try:
            # 获取百度识别的结果
            _baidu_result = self.baidu_ocr(new_image)
            # print(_baidu_result)
            if _baidu_result and "words_result" in _baidu_result:
                baidu_words = ""
                for t in range(len(_baidu_result["words_result"])):
                    baidu_words = baidu_words + _baidu_result['words_result'][t]['words']
                baidu_raw_result = json.dumps(_baidu_result, ensure_ascii=False)
            else:
                # P1漏洞
                self.error_next(_file_plus, _file_has_ext)
                traceback.print_exc()
                return None

        except Exception as e:
            # P0漏洞
            print("图片识别问题：")
            print(e)
            traceback.print_exc()
            return None

        if not self.save(pic_md5, pic_length, baidu_words, baidu_raw_result, reg + '.jpg', old_name):
            self.error_next(_file_plus, _file_has_ext)
            return None

        shutil.move(_file_plus, output_path)
        if _file_plus != (source_path + new_name).lower():
            os.remove(source_path + new_name)

    @staticmethod
    def error(pic_name):
        shutil.move(pic_name, error_pth)
        # if _file_plus != (source_path + new_name).lower():
        #     os.remove(source_path + new_name)

    @staticmethod
    def error_next(pic_name, has_ext):
        shutil.move(has_ext, error_pth)
        if has_ext != pic_name.lower():
            os.remove(pic_name)

    @staticmethod
    def rename_mode(ikey):

        f_name, ext = os.path.splitext(ikey)
        reg = str(SnowId(1, 2, 0).get_id())[1:-len(str(os.getpid()))] + str(os.getpid())
        print(reg)
        new_name = reg + ext
        os.rename(source_path + ikey, source_path + new_name)
        return new_name, f_name, reg

    @staticmethod
    def baidu_ocr(sf):
        # 调用通用文字识别（高精度版）
        options = {"detect_direction": "true", "probability": "true"}
        try:
            res = client.basicAccurate(sf, options)
        except Exception as e:
            print(e)
            res = None
        return res

    @staticmethod
    def save(md5, length, words, raw_result, new_name, old_name):

        save_sql = "INSERT INTO bus_pic_search(pic_name, pic_url, pic_md5, pic_length, pic_type, pic_contain, pic_raw) " \
                   "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (pymysql.escape_string(old_name), new_name, md5, length, "0", pymysql.escape_string(words), pymysql.escape_string(raw_result))
        return db_handle.modify_DB(save_sql)

    def run(self):
        for root, dirs, files in os.walk(source_path):
            pool = Pool(5)
            manager = Manager()
            lock = manager.Lock()
            abc = partial(self.body, lock)
            pool.map(abc, files)

if __name__ == '__main__':
        avg = PushInto()
        avg.run()