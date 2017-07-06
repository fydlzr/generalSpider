#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-01-10 11:44:51
import langdetect
import os
from utils import *

#TODO - relative issues -http://stackoverflow.com/questions/16981921/relative-imports-in-python-3
#from .parser.cx import *
#from .parser.css import *
#from .parser.index import *
#from .parser.newspaper import *

from cx import *
from css import *
from index import *
from paper import *

#paperChain = [ CSSParser , NewspaperParser , CXParser , IndexParser ]
class newsparser:
    _parserchain = []

    def __init__(self, parserchain=None):
        if parserchain:
            self._parserchain = parserchain
        if not self._parserchain or len(self._parserchain)<1:
            #self._parserchain.append(css(os.path.dirname(__file__) + './css_googlenews.json'))
            #self._parserchain.append(css(os.path.dirname(__file__) + './css_baidunews.json'))
            self._parserchain.append(paper())
            self._parserchain.append(cx())
            self._parserchain.append(index())

    def IsValidTDC(self, item):
        bValid =  len(getsafedictvalue(item,'value/title',''))>0 and \
               len(getsafedictvalue(item,'value/content',''))>0 and \
               len(getsafedictvalue(item,'value/pdate',''))>0
        return bValid

    def parse(self, data):
        retItem = {
            'response':'',
            'value': {
                'title': '',
                'pdate': '',
                'content': '',
                'showcontent': '',
                'lang':''
            },
            'parser': {
                'title': '',
                'pdate': '',
                'content': '',
                'showcontent': ''
            }
        }
        for parser in self._parserchain:
            item = parser.parse( data )
            if len(getsafedictvalue(retItem,'value/title',''))<1 and  len(getsafedictvalue(item,'title',''))>0:
                retItem['value']['title']=getsafedictvalue(item,'title','')
                retItem['parser']['title']=getsafedictvalue(item,'parser','')
            if len(getsafedictvalue(retItem,'value/content',''))<1 and  len(getsafedictvalue(item,'content',''))>0:
                retItem['value']['content']=getsafedictvalue(item,'content','')
                retItem['parser']['content']=getsafedictvalue(item,'parser','')
            if len(getsafedictvalue(retItem,'value/showcontent',''))<1 and  len(getsafedictvalue(item,'showcontent',''))>0:
                retItem['value']['showcontent']=getsafedictvalue(item,'showcontent','')
                retItem['parser']['showcontent']=getsafedictvalue(item,'parser','')
            if len(getsafedictvalue(retItem,'value/pdate',''))<1 and  len(getsafedictvalue(item,'pdate',''))>0:
                retItem['value']['pdate']=getsafedictvalue(item,'pdate','')
                retItem['parser']['pdate']=getsafedictvalue(item,'parser','')

            if self.IsValidTDC(retItem):
                retItem['response'] = 'SUCCESS'
                break
        #TODO : add clean <font ... > </font> tags in title and content for lang detect accuracy
        if len(retItem['value']['title'])>0:
	    try:
                langtitle = langdetect.detect(retItem['value']['title'])
                retItem['value']['lang'] = langtitle
	    except:
		retItem['value']['lang'] = 'unknown'
        return retItem
