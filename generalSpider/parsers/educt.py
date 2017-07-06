#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-01-10 11:44:51
import tornado.web
import tornado.httpclient
import tornado.gen
import tornado.escape
import json

from utils import *

def getvibes(autnrsp):
    res = {
        "RD_EDC_POSITIVE_VIBE":[],
        "RD_EDC_NEGATIVE_VIBE":[],
        "RD_EDC_POSITIVE_PERCENT_VIBE":0,
        "RD_EDC_NEGATIVE_PERCENT_VIBE":0,
        "RD_EDC_NEUTRAL_PERCENT_VIBE":0,
        "RD_EDC_INDICATES_VIBE":""
    }
    if getsafedictvalue(autnrsp,'autnresponse/response/$','') != 'SUCCESS':
        return res
    totalhits = int(getsafedictvalue(autnrsp,'autnresponse/responsedata/autn:numhits/$','0'))
    #print totalhits

    hit = getsafedictvalue(autnrsp,'autnresponse/responsedata/autn:hit',None)
    if not hit:
        return res
    if isinstance(hit, list):
        hits = hit
    else:
        hits = (hit, )
    #print len(hits)

    for hit in hits:
        if getsafedictvalue(hit,'entity_name/$','').startswith('sentiment/positive/'):
            res["RD_EDC_POSITIVE_VIBE"].append(getsafedictvalue(hit,'original_text/$',''))
        if getsafedictvalue(hit,'entity_name/$','').startswith('sentiment/negative/'):
            res["RD_EDC_NEGATIVE_VIBE"].append(getsafedictvalue(hit,'original_text/$',''))
    posCount = len(res["RD_EDC_POSITIVE_VIBE"])
    negCount = len(res["RD_EDC_NEGATIVE_VIBE"])

    percentPos = 0
    percentNeg = 0
    percentNeu = 0
    if (posCount+negCount)>0:
        percentPos = int((posCount*1.0/(posCount+negCount))*100)
        percentNeg = int((negCount*1.0/(posCount+negCount))*100)
        percentNeu = 100 - (percentNeg+percentPos)

    indicate = "Neutral"
    if (percentPos>=60):
        indicate = "Positive"
    elif (percentNeg>=60):
        indicate = "Negative"
    else:
        indicate = "Neutral"
    res["RD_EDC_POSITIVE_PERCENT_VIBE"] = percentPos
    res["RD_EDC_NEGATIVE_PERCENT_VIBE"] = percentNeg
    res["RD_EDC_NEUTRAL_PERCENT_VIBE"] = percentNeu
    res["RD_EDC_INDICATES_VIBE"] = indicate

    print_and_log ("<< -- educt stat: vibe: %s  pos:%d  neg: %d " % (indicate,posCount,negCount))

    return res


def get_educt(eductserver, edcstr):
    try:
        #txtedc =  tornado.escape.url_escape(item['value']['title'] + ' ' +  item['value']['content'])
        txtedc =  tornado.escape.url_escape(edcstr)

        #"http://192.168.1.2:14000/action=EduceFromText&Responseformat=json&Text=" + txtedc
        url_edc = eductserver + "/action=EduceFromText&Responseformat=json&Text=" + txtedc

        client = tornado.httpclient.HTTPClient()
        response = client.fetch(url_edc)
        #client = tornado.httpclient.AsyncHTTPClient()
        #response = yield tornado.gen.Task(client.fetch, url_edc)

        edc = getvibes(json.loads(response.body))
        #print_and_log(edc)
        print_and_log ("<< 2 educt succeed.")
        return edc
    except Exception as e:
        print_and_log ("xx 2 error educt")
        #print_and_log(e)
        return None