# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
from Crawler.settings import *
import re
import os
import pymongo
from Crawler.util import RedisFactory
from Crawler.items import NewsItem, CommentItem
from Crawler.items import TweetsItem, InformationItem, FollowsItem, FansItem
from xml.etree import ElementTree as ET

class CrawlerPipeline(object):
    def __init__(self):
        # self.file = open('items.jl', 'w', encoding='utf-8')
        self.url_seen = set()
        # self.redis = RedisFactory('url')

    def process_item(self, item, spider):
        if item['url'] in self.url_seen:
            raise print("Duplicate item found: %s" % item)
        else:
            self.url_seen.add(item['url'])
            # line = json.dumps(dict(item), ensure_ascii=False)+"\n"

            domainname = item['domainname']
            chinesename = item['chinesename']
            pinyin = item['chinesename']
            webencoding = 'utf-8'
            language = '维文'
            encodingtype = '默认'
            registrationnumber = '默认'
            provider = item['domainname']
            corpustype = '网络'

            title = item['title']
            subtitle = ''
            author = ''
            timeofpublish = item['timeofpublish']
            timeofdownload = ''

            clicktimes = '0'
            codeofclassification = ''
            nameofclassification = ''
            methodofclassification = ''
            keywordsofclassification =''
            content = item['content']
            column = ''

            root = ET.Element('file')
            domainname_node = ET.SubElement(root, 'domainname')
            domainname_node.text = domainname
            chinesename_node = ET.SubElement(root, 'chinesename')
            chinesename_node.text = chinesename

            pinyin_node = ET.SubElement(root, 'pinyin')
            pinyin_node.text = pinyin
            webencoding_node = ET.SubElement(root, 'webencoding')
            webencoding_node.text = webencoding
            language_node = ET.SubElement(root, 'language')
            language_node.text = language

            encodingtype_node = ET.SubElement(root, 'encodingtype')
            encodingtype_node.text = encodingtype
            registrationnumber_node = ET.SubElement(root, 'registrationnumber')
            registrationnumber_node.text = registrationnumber

            provider_node = ET.SubElement(root, 'provider')
            provider_node.text = provider
            corpustype_node = ET.SubElement(root, 'corpustype')
            corpustype_node.text = corpustype

            # 写入文档中
            time_str = item['timeofpublish'][:10]
            if '年' in time_str:
                time_str = datetime.datetime.strptime(item['timeofpublish'], "%Y年%m月%d日%H:%M").strftime('%Y-%m-%d')
            m = re.search(r'\d{2}-\d{2}-\d{4}', time_str)
            m1 = re.search(r'\d{4}.\d{2}.\d{2}', time_str)
            flag = 0
            if m:
                time_str = m.group(0)
                time_str_tmp = time_str.split("-")
            elif m1:
                time_str = m1.group(0)
                flag = 1

            if flag != 1:
                time_path = time_str_tmp[2] + "\\" + time_str_tmp[1] + "\\" + time_str_tmp[0]
            else:
                time_path = time_str.replace('.', '\\')

            path = SAVE_PATH  + "wei" + "\\" +time_path + '\\'
            if not os.path.exists(path):
                os.makedirs(path)
            url = item['url'].replace('http:/', '_').replace('/', '_').replace(':', '')

            if flag != 1:
                tmp = time_str_tmp[2] + "-" + time_str_tmp[1] + "-" + time_str_tmp[0]
            else:
                tmp = time_str.replace('.', '-')

            filename = '0_' + spider.name + '_' + tmp + url +'.xml'

            filename_node = ET.SubElement(root, 'filename')
            filename_node.text = filename
            title_node = ET.SubElement(root, 'title')
            title_node.text = title

            subtitle_node = ET.SubElement(root, 'subtitle')
            subtitle_node.text = subtitle
            author_node = ET.SubElement(root, 'author')
            author_node.text = author

            timeofpublish_node = ET.SubElement(root, 'timeofpublish')
            timeofpublish_node.text = timeofpublish
            timeofdownload_node = ET.SubElement(root, 'timeofdownload')
            timeofdownload_node.text = timeofdownload

            url = item['url']
            url_node = ET.SubElement(root, 'url')
            url_node.text = url
            clicktimes_node = ET.SubElement(root, 'clicktimes')
            clicktimes_node.text = clicktimes

            codeofclassification_node = ET.SubElement(root, 'codeofclassification')
            codeofclassification_node.text = codeofclassification
            nameofclassification_node = ET.SubElement(root, 'nameofclassification')
            nameofclassification_node.text = nameofclassification

            methodofclassification_node = ET.SubElement(root, 'methodofclassification')
            methodofclassification_node.text = methodofclassification
            keywordsofclassification_node= ET.SubElement(root, 'keywordsofclassification')
            keywordsofclassification_node.text = keywordsofclassification

            content_node = ET.SubElement(root, 'content')
            content_node.text = content
            column_node = ET.SubElement(root, 'column')
            column_node.text = column

            tree = ET.ElementTree(root)

            tree.write(path + filename)
        return item

    def close_spider(self, spider):
        self.file.close()


class MongoDBPipleline(object):
    def __init__(self):
        clinet = pymongo.MongoClient("localhost", 27017)
        db = clinet["Sina"]
        self.Information = db["Information"]
        self.Tweets = db["Tweets"]
        self.Follows = db["Follows"]

    def process_item(self, item, spider):
        """ 判断item的类型，并作相应的处理，再入数据库 """
        if isinstance(item, InformationItem):
            try:
                self.Information.insert(dict(item))
            except Exception:
                pass
        elif isinstance(item, TweetsItem):
            try:
                self.Tweets.insert(dict(item))
            except Exception:
                pass
        elif isinstance(item, FollowsItem):
            try:
                self.Follows.insert(dict(item))
            except Exception:
                pass
        return item


class MongoPipeline(object):

    db_name = 'all_news'
    db_comment = 'all_comments'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if isinstance(item, NewsItem):
            key = {'url': item['url']}
            self.db[self.db_name].update(key, dict(item), upsert=True)
            return item
        elif isinstance(item, CommentItem):
            key = {'url': item['url']}
            self.db[self.db_comment].update(key, dict(item), upsert=True)
            return 'comments of ' + item['url']