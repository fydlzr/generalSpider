# -*- coding: utf-8 -*-
import scrapy
from scrapy.selector import  Selector
from scrapy.http import Request
from generalSpider.xpaths import xpaths, newsXpaths, blogXpaths
from newspaper import Article
import chardet
import time
import tldextract
import dateparser
from generalSpider.parsers.newsparser import *
from scrapy.conf import settings
import base64
from scrapy.exceptions import DropItem
noFix = set(['pdf','doc','docx','xls','xlsx','doc','docs'])
date1 = re.compile(r'(2\d{3})年(0{0,1}\d{1}|1[0-2])月(0{0,1}\d{1}|[12]\d{1}|3[01])日'.decode('utf-8'))
date2 = re.compile(r'(2\d{3})-(0{0,1}\d{1}|1[0-2])-(0{0,1}\d{1}|[12]\d{1}|3[01])')
dateurl = re.compile(r'(2\d{3})(0\d{1}|1[0-2])(0\d{1}|[12]\d{1}|3[01])')
dateurl2 = re.compile(r'(2\d{3})/(0{0,1}\d{1}|1[0-2])/(0{0,1}\d{1}|[12]\d{1}|3[01])')
time1 = r'((0[1-9])|(1[0-9])|(2[0-3])|([1-9]))\:([0-5][0-9])((\:([0-5][0-9]))|)'

class GeneralspiderSpider(scrapy.Spider):
    name = "generalspider"
    allowed_domains = []
    start_urls = []
    selector = {}
    myPipeline = None
    proxyipport = ''
    proxyuserpass = ''
    SpiderUrlCantHaveRegex = []
    SpiderUrlMustHaveRegex = []
    PageCantHaveRegex=[]
    PageMustHaveRegex=[]
    UrlCantHaveRegex=[]
    UrlMustHaveRegex=[]
    PageDateSelector=''
    PageTitleSelector=''
    PageContentSelector=''

    def __init__(self, \
		ProxyHost=None ,ProxyPort=80 ,ProxyUser='username' ,ProxyPassword='password',\
		Url='',Depth=-1,StayOnSite=True,DownloadDelay=0.5,ConReqPerDomain=16,SpiderUrlCantHaveRegex='',SpiderUrlMustHaveRegex='',\
		PageCantHaveRegex='',PageMustHaveRegex='',UrlCantHaveRegex='',UrlMustHaveRegex='',PageDateSelector='',PageTitleSelector='',PageContentSelector='',\
		RepoType='mongo',RepoHost='app.raydata.cn',RepoPort=23927,RepoDatabaseName='test',\
		*args, **kwargs):

        super(GeneralspiderSpider, self).__init__(*args, **kwargs)
	if ProxyHost == None:
		self.proxyipport = None
		self.proxyuserpass = None
	else:
		self.proxyipport = 'http://'+ProxyHost + ':' +str(ProxyPort)
		self.proxyuserpass = ProxyUser + ':' + ProxyPassword
	self.SpiderUrlCantHaveRegex=SpiderUrlCantHaveRegex.split('，')
        self.SpiderUrlMustHaveRegex=SpiderUrlMustHaveRegex.split('，')
	self.PageCantHaveRegex=PageCantHaveRegex.split('，')
	self.PageMustHaveRegex=PageMustHaveRegex.split('，')
	self.UrlCantHaveRegex=UrlCantHaveRegex.split('，')
	self.UrlMustHaveRegex=UrlMustHaveRegex.split('，')
	self.PageDateSelector=PageDateSelector
	self.PageTitleSelector=PageTitleSelector
	self.PageContentSelector=PageContentSelector

	if StayOnSite!=True and 'true' in StayOnSite.lower():
		StayOnSite = False
	else:
		StayOnSite = True
		
	
	settings['DEPTH_LIMIT'] = Depth
	settings['DOWNLOAD_DELAY'] = DownloadDelay
	settings['CONCURRENT_REQUESTS_PER_DOMAIN'] = ConReqPerDomain
	
	if RepoType =='mongo':
		settings['DB_SERVER'] =RepoHost
        	settings['DB_PORT'] = int(RepoPort)
	        settings['DB_DB'] = RepoDatabaseName
	
        start_urls = Url.split('，')
        for start_url in start_urls:
                ext = tldextract.extract(start_url)
		if 'http' not in start_url:
			start_url = 'http://' + start_url
                normUrl = '.'.join(e for e in ext if e)
                self.start_urls.append(start_url)
		if StayOnSite:
	                self.allowed_domains.append('.'.join(e for e in ext[1:] if e))
    def start_requests(self):
	for u in self.start_urls:
		if self.proxyipport!=None:
			yield scrapy.Request(u,callback=self.parse,  meta={'proxy':self.proxyipport})
		else:
			yield scrapy.Request(u,callback=self.parse)
		'''
		request.meta['proxy'] = self.proxyipport
		encoded_user_pass = base64.encodestring(self.proxyuserpass)
		request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass
		'''
    def parse(self, response):
        body = response.body
        if 'GB' in chardet.detect(response.body)["encoding"]:
                body = response.body.decode('gb18030').encode('utf-8')
        try:
                sel = Selector(text=body)
        except:
                noFix.add(response.url.split('.')[-1])
                return
        links_in_a_page = sel.xpath('//a/@href').extract()
	#print(len(links_in_a_page))
        for link in links_in_a_page:
            if link:
                if not link.startswith('http'):  # 处理相对URL
                    link = response.urljoin(link).strip('/')
                endfix = link.split('.')[-1]
                if endfix in noFix:
                        continue
                if self.myPipeline.url_seen(link)==False:
			if self.spiderUrlCheck(link):
			    #print link
			    #continue
			    if self.proxyipport!=None:
        	                yield scrapy.Request(link, callback=self.parse,  meta={'proxy':self.proxyipport})
                	    else:
				yield scrapy.Request(link, callback=self.parse)
			#else:
			#	print link
        item = {'url': response.url, 'title':'', 'pdate':'', 'showcontent':'', 'content':'', 'parser':{}}
	if self.PageTitleSelector!='' and self.PageDateSelector!='' and self.PageContentSelector!='':
		selector = {'title':[self.PageTitleSelector], 'pdate':[self.PageDateSelector], 'content':[self.PageContentSelector]}
	else:
	        selector = self.getSelector(response.url)
        if selector!=None:
                for key in selector:
                        if type(selector[key])!=list:
                                item[key] = selector[key]
                                continue
                        res = None
                        for Sel in selector[key]:
                                res = self.getNodeText(sel.xpath(Sel))
                                if len(res.strip())>0:
                                        break 
                                else:
                                        if 'tbody' in Sel:
                                                Sel = Sel.replace('/tbody','')
                                                res = self.getNodeText(sel.xpath(Sel))
                                                if len(res.strip())>0:
                                                        #tb = open('tb.txt','a')
                                                        #tb.write(item['url']+'\t' + key +'\n')
                                                        break
                        item[key] = res
                        #print key + res
                        if len(item[key])>0:
                                item['parser'][key] = 'xpath'
                item['showcontent'] = self.cleanBR_RN(item['showcontent'], '<br/>')
                item['content'] = ('\n'.join(t for t in item['showcontent'].split('\n') if 'img' not in t and 'http' not in t)).encode('utf-8')
		item['pdate'] = self.getDate(item['pdate'])
        #print item['pdate'], item['title'],len(item['showcontent'])
	#print item['showcontent']
	if item['pdate']==None or len(item['pdate'])<6:
                item['parser']['pdate'] = 'rule'
                item['pdate'] = self.getDate(item['showcontent'])
        if item['pdate']==None or len(item['pdate'])<6:
                item['pdate'] = self.getDateFromURL(response.url)
        if item['pdate']==None or len(item['pdate'])<6:
                item['pdate'] = self.getDate(body)
        if selector==None or item['pdate']==None or len(item['pdate'])<6 or item['title']=='' or item['content']=='':
                it = newsparser().parse({'html':body, 'url':response.url})
                for p in it['parser']:
                        if p not in item['parser']:
                                item['parser'][p] = it['parser'][p]
                item['lang']='zh'
                if selector==None:
                        item = it['value']
                        item['parser'] = it['parser']
                        item['showcontent'] = self.cleanBR_RN(item['showcontent'], "<br/>")
                        item['content'] = self.cleanBR_RN(item['content'], "")
                else:
                        it = it['value']
                        if item['title'] == '':
                                item['title'] = it['title']
                        if item['pdate']==None or len(item['pdate'])<6:
                                item['pdate'] = self.getDate(it['pdate'])
                        if item['content'] == '':
                                item['content'] = it['content']
                                item['showcontent'] = it['showcontent']        
        item['source'] = self.start_urls[0]
        item['region'] = ''
        item['src'] = self.start_urls[0]
        item['edc'] = {}
        item['language'] = 'UTF8'
        item['tags'] = ''
        item['database'] = ''
        item['crawlername'] = self.name
        item['image_url'] = ''
        item['image_data'] = ''
        item['crawltime'] = int(time.time())
        item['type'] = u'房地产'
        try:
                item['pdate'] = int(item['pdate'])
        except:
                item['pdate']=None
	if self.urlCheck(item['url']) and self.pageCheck(item['content']):
	        yield item
	else:
		if not self.urlCheck(item['url']):
			raise DropItem("write to DB urlCheck Failed: %s" % item['url'])
		else:
			raise DropItem("write to DB pageCheck Failed: %s" % item['url'])
    def urlCheck(self, url):
	for cant in self.UrlCantHaveRegex:
		if cant!='' and cant in url:
			return False
	for must in self.UrlMustHaveRegex:
		if must!='' and must not in url:
			return False
	return True

    def pageCheck(self, content):
	for cant in self.PageCantHaveRegex:
                if cant!='' and cant in content:
                        return False
        for must in self.PageMustHaveRegex:
                if must!='' and must not in content:
                        return False
        return True

    def spiderUrlCheck(self, url):
	#print self.SpiderUrlCantHaveRegex
	#print self.SpiderUrlMustHaveRegex
	for cant in self.SpiderUrlCantHaveRegex:
		if cant == '': continue
                if cant in url:
                        return False
        for must in self.SpiderUrlMustHaveRegex:
		if must == '': continue
                if must not in url:
                        return False
        return True
    def getNodeText(self, sel):
        if type(sel)==Selector:
		uq = sel.extract_unquoted()
                if '<style' not in uq and '<script' not in uq:
			if '/@' in str(sel):
                                return sel.extract().strip()
                        else:
                                return '\n'.join(sel.xpath('string(.)').extract()).strip()
		children = sel.xpath('./*')
                if len(children)==0:
                        if '<style' in uq or '<script' in uq:
                                return ''
		subtext = []
		for child in children:
			t = self.getNodeText(child).strip()
			if t!='':
				subtext.append(t)
		return ' '.join(subtext).strip()	
        else:
                text = []
                for se in sel:
                        text.append(self.getNodeText(se ))
                return '\t'.join(text).strip()

    def getDate(self, line):
        if line==None or line=='':
                return None
        if type(line)==int:
                return line
	try:
		dint = int(line)
		return str(dint)
	except:
		pass
	try:
		t = line
		if ':' in line:
			t = line.split(':')[1].strip()
		if len(t)>6:			
			t = dateparser.parse(t)
        	        dateint = int(time.mktime(t.timetuple()))
                	return str(dateint)
	except:
		pass
        line = line.replace('/','-')
	line = line.replace('_','-')
        date = date1.findall(line)
        if date==None or len(date)==0:
                date = date2.findall(line)
        if date==None or len(date)==0:
                return None
        else:
		t = '-'.join(date[0])
                t1 = re.search(time1, line)
                if t1 != None:
                        t1 = t1.group()
                        t = t + ' ' + t1
                t = dateparser.parse(t)
                if t == None:
                        return None
                dateint = int(time.mktime(t.timetuple()))
                return str(dateint)
    def getDateFromURL(self, line):
        date = dateurl.findall(line)
        if date==None or len(date)==0:
                date = dateurl2.findall(line)
        if date==None or len(date)==0:
                return None
        else:
                t = dateparser.parse('-'.join(date[0]))
                if t == None:
                        return None
                dateint = int(time.mktime(t.timetuple()))
                return str(dateint)
    
    def cleanBR_RN(self, content, char):
        lines = re.split(char + '\t|\n|\r' if char=='' else '|\t|\n|\r',content)
        res = ''
        if char == '':
                char = '\n'
        for line in lines:
                line = line.strip()
                if len(line)>1:
                        res += line + char
        return res
    def getSelector(self, start_url):
        ext = tldextract.extract(start_url)
        normUrl = '.'.join(e for e in ext if e)
        if normUrl in xpaths:
                selector = xpaths[normUrl]
        elif 'www.'+'.'.join(e for e in ext[1:] if e) in xpaths:
                selector = xpaths['www.'+'.'.join(e for e in ext[1:] if e)]
        else:
		for xpath in blogXpaths:
                        if xpath in start_url:
                                return blogXpaths[xpath]
                for xpath in newsXpaths:
                        if xpath in start_url:
                                return newsXpaths[xpath]
        return selector
