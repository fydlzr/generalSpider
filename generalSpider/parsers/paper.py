# -*- coding: utf-8 -*-
from newspaper import *

from doc import *
from utils import *
from dateextract import *

class paper:
    def parse(self, params ):
        item = {
            'parser' : 'Newspaper',
            'title':   '',
            'pdate':   '',
            'content': '',
            'showcontent': ''
        }
        html = params['html']
        url = params['url']

        try:
            docrsp = doc(html,url)
            config = Config()
            config.fetch_images = False
	    config.language = 'zh'
            first_article = Article(url=url, config=config)
            first_article.download(docrsp.html())
            first_article.parse()
            #do not use the title extract
            #item['title'] = first_article.title
            pubtime = first_article.publish_date
            if pubtime:
                pubtimeint = int(time.mktime(pubtime.timetuple()))
                if (pubtimeint > 0):
                    item['pdate'] = ValidateTime(pubtimeint)
            item['content'] = first_article.text
            item['showcontent'] = item['content'].replace("\n", "<br/>")
        except Exception as e:
            pass
        return item
