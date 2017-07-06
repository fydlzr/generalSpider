# -*- coding: utf-8 -*-
import tldextract
import lxml.html.clean as clean
import json
import codecs
from doc import *
from utils import *
from dateextract import *


class css:
    _parserTable = None
    _css = './gspider_parser.json'

    def __init__(self, css):
        self._css = css
        self._parserTable = self.initCSSParser(css)
    def initCSSParser(self, css):
        _parserTable = None
        with codecs.open(css,encoding='utf-8') as data_file:
            _parserTable = json.load(data_file)
        return _parserTable

    def parse(self, params ):
        item = {
            'parser' : 'CSS',
            'title':   '',
            'pdate':   '',
            'content': '',
            'showcontent': ''
        }
        parserTable = self._parserTable
        html = params['html']
        url = params['url']

        try:
            url_ext = tldextract.extract(url).domain
            parser = getsafedictvalue(parserTable, url_ext+"/parser", None)
            cnname = getsafedictvalue(parserTable, url_ext + "/name", "")
            if parser is None:
                return item

            linkurl = url
            docrsp = doc(html,url)
            pubtimeint = 0
            pubtimetxt=''
            for CSS in parser:
                contraw = docrsp(CSS["content"]).remove("a").remove("script").remove("style")
                if contraw == None:
                    continue
                item['content'] = contraw.text()
                item['title'] = docrsp(CSS["title"]).text()
                pubtimetxt = docrsp(CSS["date"]).text()
                pubtimeint = parsedate(pubtimetxt)
                if (len( item['content']) > 0) and (len(item['title']) > 0) and (pubtimeint > 0):
                    break;

            if contraw:
                cleaner = clean.Cleaner(page_structure=True)
                showcont = cleaner.clean_html(contraw.remove_attr('id').remove_attr('class').wrapAll('<div></div>').html())
                showcont = re.sub(r'id=".*?"|class=".*?"', '', showcont)
                showcont = re.sub(r'[\s+]*?>', '>', showcont)
                showcont = showcont.replace("\n", "").replace("\t", "").replace("<div>", "").replace("</div>", "")
                item['showcontent'] = showcont
                if (pubtimeint > 0):
                    item['pdate'] = ValidateTime(pubtimeint)
        except Exception as e:
            pass
        return item
