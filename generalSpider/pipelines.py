# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
from scrapy.exceptions import DropItem
import pymongo
from scrapy.conf import settings
from pybloom import BloomFilter
import hashlib
import tldextract
from pymongo import DESCENDING
import time
from generalSpider.keywords import keywords_ZH, keywords_EN
filetype = ['htm', 'asp', 'jsp', 'php', 'xml']
class GeneralspiderPipeline(object):
    def __init__(self):
        self.faillog = open('fail.txt','a')
        #self.urls_seen = set()
        if os.path.exists(settings['DB_DB']+'.urls'):
            self.bloomFilter = BloomFilter.fromfile(open(settings['DB_DB']+'.urls','r'))
        else:
            self.bloomFilter = BloomFilter(1000000,0.001)
        connection = pymongo.MongoClient(
            settings['DB_SERVER'],
            settings['DB_PORT']
        )
        db = connection[settings['DB_DB']]
        self.collection = db[settings['DB_COLLECTION']]
        self.collection.ensure_index('url', unique=True)
        self.collection.create_index([("crawltime", DESCENDING)])
        self.url2name = self.loadDict(connection)

    def loadDict(self, con):
        db = con['config']
        col = db['dict']
        return col.find().next()


    def process_item(self, item, spider):
        md5 = self.get_md5(item['url'])
        if self.url_seen(md5):
            raise DropItem("Duplicate item found: %s" % item['url'])
        else:
                self.url_add(md5)

        if self.myFilter(item):
                item['showcontent'] = str(len(item['showcontent'])) + '!!'*10
                item['content'] = "content"
                raise DropItem("Filted: %s" % item['url'])
        if self.hasKeyword(item['lang'],item['content']):
            self.update(item)
            if item['src']==item['title']:
                raise DropItem("title WRONG!!!Filted: %s" % item['url'])
            curtime = time.strftime("%Y%m%d")
            if curtime != self.collection.name:
                self.collection = db[settings[curtime]]
                self.collection.ensure_index('url', unique=True)
                self.collection.create_index([("crawltime", DESCENDING)])
            self.collection.insert(dict(item))
        else:
            #print 'no keywords: '+ item['url']
            raise DropItem("no Keywords:%s" %item['url'])
        return item

    def update(self, item):
        ext = tldextract.extract(item['url'])
        normurl = '.'.join(e for e in ext if e)
        if normurl in self.url2name:
                item['src'] = self.url2name[normurl]
        

    def myFilter(self, item):
        if item['src']==item['title']:
                return True
        u = item['url'].strip('/').lower()
        flag = False
        for ft in filetype:
                if ft in u:
                        flag = True
                        break
        if flag==False: return True
        if u.endswith(r'.cn') or u.endswith(r'.com') or u.endswith('index.html'):
		raise DropItem("URL endsWITH  Fail: %s" % item['url'])
                return True
        if ('list' in u or 'index' in u) and (len(item['title'])<8 or len(item['content'])<50):
		raise DropItem("URL contains list/index  Fail: %s" % item['url'])
                return True
        if len(item['title'])<6 or item['pdate'] =='' or item['pdate'] == None or item['content'] == '':
		raise DropItem("title/pdate/showcontent Fail: %s" % item['url'])
                #print item['url'] + ':' + item['title'][:9] + '---' + str(item['pdate']) + '---' + str(len(item['content'])) + '='*50
                self.faillog.write(item['url']+'\t\t' + item['url'] + '\t' + item['title'][:9] + '\t' + str(item['pdate']) + '\t' + str(len(item['content']))  + '\n')
                return True
        return False

    #False:unSeen : True:Seen
    def get_md5(self, url):
        m2 = hashlib.md5()
        m2.update(url)
        return m2.hexdigest()

    def url_add(self, url_md5):
        self.bloomFilter.add(url_md5)

    def url_seen(self, url_md5):
        #return False
        return url_md5 in self.bloomFilter

    def open_spider(self, spider):
        spider.myPipeline = self
        #for url in self.collection.find({},{"url":1,"_id":0}):
        #        self.urls_seen.add(url["url"])

    def close_spider(self, spider):
        self.faillog.close()
        self.bloomFilter.tofile(open(settings['DB_DB']+'.urls','wb'))

    def hasKeyword(self, lang, text):
	keywords = None
        if lang=='en':
                keywords = keywords_EN
        else:
                keywords = keywords_ZH
        for key in keywords:
            if key in text:
                return True
        return False
