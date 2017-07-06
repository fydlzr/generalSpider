#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-01-10 11:44:51

import datetime
from utils import *

def stat_redis(host, port, dbi, key):
    import redis
    r=redis.StrictRedis(host,port,db=dbi)
    r.incr(key)
def stat_perday(host, port, dbi, db):
    try:
        key = utcYMD(datetime.datetime.utcnow())
        return stat_redis(host, port, dbi, db + key)
    except Exception as e:
        #print e
        pass
