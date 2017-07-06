# -*- coding: utf-8 -*-
from doc import *
from utils import *
from dateextract import *

class index:
    def parse(self,params):
        item = {
            'parser' : 'Index',
            'title':   '',
            'pdate':   '',
            'content': '',
            'showcontent': ''
        }

        try:
            item['title'] = params['title']
            item['pdate'] = ValidateTime( datefromgoogle(params['date']))
        except Exception as e:
            pass
        return item

