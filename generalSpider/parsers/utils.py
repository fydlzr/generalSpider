#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-01-10 11:44:51
import datetime
import time
import logging

def print_and_log(msg):
    try:
        print (msg)
        logging.info(msg)
    except Exception as e:
        pass

def utctimestamp(utcdate):
    return int(time.mktime(utcdate.timetuple()))
def utcYMD(utcdate):
    return utcdate.strftime("%Y%m%d")

def gen_suc_rsp(item):
    return {
        "response": "SUCCESS",
        "responsedata": item
    }

def gen_fail_rsp(item):
    return {
        "response": "FAIL",
        "error": item
    }

def getsafedictvalue(dict,pathval,defval):
    childs = pathval.split("/")
    childdict = dict
    try:
        for child in childs:
            childdict = childdict[child]
        retval = childdict
    except Exception as e:
        retval = defval
        pass
    return retval

def ValidateTime( pubtimeint ):
    nowt = datetime.datetime.now()
    limit = datetime.datetime(nowt.year,nowt.month,nowt.day,23,59,59)
    if pubtimeint > int(time.mktime(limit.timetuple())):
        return ''
    return str(pubtimeint)