"""
A股 巨潮咨询爬虫 cninfo.com.cn
"""

import json
import scrapy
from scrapy import signals
import requests
import uuid
import time
from datetime import datetime, timedelta
from orator import DatabaseManager
from scrapy.utils.project import get_project_settings
from test_astock.test_astock.settings import *


class JcwCninfoCrawler(scrapy.Spider):
    # 配置项目的参数
    name = 'cninfo_index'
    allowed_domains = ['cninfo.com.cn']
    custom_settings = {'CRAWLERA_ENABLED': False}
    # start_urls = ['http://cninfo.com.cn/']
    database = DATABASE

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """
        自定义组件，自定义爬虫关闭时的信号
        """
        spider = super(JcwCninfoCrawler, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_closed(self):
        """
        爬虫关闭处理
        """
        print('spider closed')

    def __init__(self, mode='update'):
        self.stock_universe = self.db_model.table('astock_platform').where('platform', 'cninfo.com.cn').get()
        self.mode = mode
        self.db_model = DatabaseManager(self.database)
        self.settings = get_project_settings

    def start_requests(self):
        s_date = '2022-02-23'
        e_date = datetime.now().strftime('%Y-%m-%d')
        reslist = self.stock_universe
        request_list = []
        url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'

        for rl_item in reslist:
            if 'org_id' in rl_item:
                tabnames = ['fulltext', 'relation']
                for tn in tabnames:
                    paradata = {'stock': rl_item['stock_code'] + ',' + rl_item['org_id'], 'tabName': tn,
                                'pageSize': '30', 'pageNum': '1', 'column': 'szse'}
                    if self.mode == 'update':
                        paradata['seDate'] = f'{s_date}~{e_date}'
                    request_list.append(scrapy.FormRequest(url=url, method='POST', formdata=paradata,
                                                             meta={'tn': tn, 'paradata':paradata},
                                                             callback=self.getOnePage, dont_filter=True))
            else:
                print(f'当前code没有org_id:{rl_item["org_id"]}')
        return request_list


    def uuidhash(self, string_value):
        return str(uuid.uuid3(uuid.NAMESPACE_DNS, string_value))

    def get_non_repeat_list(self, data, key):
        ids = [i[key] for n, i in enumerate(data)]
        return [i for n, i in enumerate(data) if i[key] not in ids[:n]]

    def script_distinct(self, new, old, key):
        new = self.get_non_repeat_list(new, key)
        return [y for y in new if y[key] not in old]

    def db_save(self, model, arr, key, table, tabs=[]):
        """
        :introduction: 数据库批量插入数据（去重后）
        :param model: 数据库连接
        :param arr: 批量插入的数据
        :param key: 插入表唯一键
        :param table: 插入表名
        :param tabs: 插入判断 [{tab: 表名, key: 对比键值}]
        :return:
        """
        keys = [i[key] for i in arr]
        old_ids = []
        for tab in tabs:
            old_ids.extend(model.table(tab['tab']).where_in(tab['key'], keys)).lists(tab['key'])
        _data = self.script_distinct(arr, old_ids, key)
        model.table(table).insert(_data)
        return _data


    def getOnePage(self, response):
        url = response.url
        tn = response.meta['tn']
        paradata = response.meta['paradata']

        if response.body is None or (type(response.body) == str and len(response.body) == 0):
            print([paradata['stock']])
            return

        data = json.loads(response.body)
        cur_page = int(paradata['pageNum'])
        total_page = data['totalpages']
        data_row = data['announcements']
        worklist = []
        weblist = []

        if data_row is None:
            print([paradata['stock'], data])
            return
        for dr in data_row:
            _url = 'http://static.cninfo.com.cn/' + dr['adjunctUrl']
            _id = self.uuidhash(_url)

            web = {}
            web['business_key'] = _id
            web["status"] = "open"
            web["mid"] = None
            web["module_source"] = tn
            web["online_qa"] = False
            web["created_at"] = datetime.now()
            weblist.append(web)


            item = {}
            item['id'] = _id
            item['url'] = _url
            item['published'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(float(dr["announcementTime"]) / 1000)))
            item['title_cn'] = dr["announcementTitle"]
            item['stock_code'] = dr["secCode"]
            item['source_file'] = "http://static.cninfo.com.cn/" + dr["adjunctUrl"]
            item['org_code'] = dr["orgId"]
            item['org_name'] = dr["orgName"]
            item['platform'] = "www.cninfo.com.cn"
            item["created_at"] = datetime.now()
            item["event_type"] = "brd/unknown"
            item['bpm_status'] = 'ready_for_start'
            worklist.append(item)

        if len(weblist) > 0:
            self.db_save(self.db_model, weblist, 'business_key', 'astock_crawler', [{'tab': 'astock_crawler', 'key': 'business_key'}])
        if len(worklist) > 0:
            self.db_save(self.db_model, worklist, 'id', 'astock_platform', [{"tab": 'astock_platform', "key": "id"}])
        print('{}的第{}页解析完成，对比去重获得数据{}条，开始写入DB'.format(paradata['stock'], cur_page, len(worklist)))

        # 翻页
        if cur_page > total_page + 1:
            return
        paradata['pageNum'] = str(cur_page + 1)
        yield scrapy.FormRequest(url=url, method='POST', formdata=paradata, meta={"tn": tn, "pardata": paradata},
                                 callback=self.getOnePage, dont_filter=True)






















