#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-01-10 11:44:51
import re
import time
import datetime
import dateutil
import dateparser

def dateparser_parse(datestr):
    dateint = 0
    try:
        pubtime =  dateparser.parse( datestr )
        dateint = int(time.mktime(pubtime.timetuple()))
    except:
        dateint = 0
    return dateint
def dateutil_parse(datestr):
    dateint = 0
    try:
        pubtime =  pubtime = dateutil.parser.parse(datestr,fuzzy=True)
        dateint = int(time.mktime(pubtime.timetuple()))
    except:
        dateint = 0
    return dateint

def parsedate(datestr):
    dateint = 0
    try:
        #del all chinese characters
        pubtimetxt2 = re.sub(r'[\u4e00-\u9fff]+', ' ', datestr)
        pubtimetxt2 = re.sub(r'[\ufe30-\uffa0]+', ' ', pubtimetxt2)
        pos_year = re.search(r'201\d\D', pubtimetxt2).regs[0][0]
        pos_time = pos_year + len("yyyy-mm-dd")
        match_hhmmss = re.search(r'\D\d\d:\d\d:\d\d',pubtimetxt2)
        match_hhmm = re.search(r'\D\d\d:\d\d',pubtimetxt2)
        if (match_hhmmss):
            pos_time = match_hhmmss.regs[0][1]
        elif (match_hhmm):
            pos_time = match_hhmm.regs[0][1]
        pubtimetxt2 = pubtimetxt2[pos_year:pos_time]
        dateint = dateutil_parse(pubtimetxt2)
    except:
        dateint = 0
    return dateint

def datefromgoogle(datestr):
    dateint = 0
    try:
        dateint = dateparser_parse(datestr)
        if dateint > 0:
            return dateint

        if not u' ' in datestr:
            dateint = dateutil_parse(datestr)
        else:
            day = datestr.split(u' ',1)[0]
            temp = datestr.split(u' ',1)[1]
            dateru = u''
            if u'時間前' == temp or u'小時前' == temp:
                dateru = day + u' час назад'
            else:
                if u'分前' == temp or u'分鐘前' == temp:
                    dateru = day + u' минут назад'
            dateint = dateparser_parse(dateru)
    except:
        dateint = 0
    return dateint