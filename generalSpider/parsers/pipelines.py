# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exceptions import DropItem
import pymongo
from scrapy.conf import settings

keywords = set(['incentive',
	'facility',
	'real estate',
	'property',
	'REIT',
	'REITs',
	'housing',
	'Chinese investors',
	'foreign investment',
	'insentif',
	'fasilitas',
	'properti',
	'perumahan',
	'pelaburan langsung asing',
	'PLA',	
])

class GovspiderPipeline(object):
    def __init__(self):
	self.urls_seen = set()
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):
	if item['url'] in self.urls_seen:
                raise DropItem('Dup url:%s' % item['url'])
        else:
                self.urls_seen.add(item['url'])
	if self.hasKeyword(item['content']):
		self.collection.insert(dict(item))
	else:
		print 'no keywords: '+ item['url']
		raise DropItem("no Keywords:%s" %item['url'])
        return item

    def open_spider(self, spider):
        spider.myPipeline = self
        for url in self.collection.find({},{"url":1,"_id":0}):
                self.urls_seen.add(url["url"])

    def hasKeyword(self, text):
        for key in keywords:
                if key in text:
                        return True
        return False
