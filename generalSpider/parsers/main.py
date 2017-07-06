# -*- coding: utf-8 -*-
import time
import logging
from utils import *
from newsparser import *
from stats import *
from educt import *

#stats
def stat(dbi, db):
    if len(options.stat)>1:
        childs = options.stat.split(":")
        host = childs[0]
        port = childs[1]
        stat_perday(host, port, dbi, db)


class parseHandler(tornado.web.RequestHandler):
    def post(self**kwargs):
        try:
            jsonstr = self.request.body#.decode('utf-8')
            data = json.loads(jsonstr)
            '''
            data = {
                'html' : data['html'],
                'url' : data['url'],
                'date' : data['date'],
                'title' : data['title'],
                'lang': data['lang'],
                'educt': data['lang']
            }
            '''
            need_educt = getsafedictvalue(data,'educt','')=='TRUE'
            stat(7, "pa-")
            stat(7, data['lang'] + "pa-")
            print_and_log(">> parsing : " + data['url'] + ' ' +data['date'] + ' ' + data['title'])
            item = newsparser().parse(data)

            rsp = {}
            pdate_text = getsafedictvalue(item,'value/pdate','')
            title_text = getsafedictvalue(item,'value/title','')
            content_text = getsafedictvalue(item,'value/content','')
            lang_text = getsafedictvalue(item,'value/lang','')
            if getsafedictvalue(item,'response','') == 'SUCCESS':
                stat(7, "pa1-")
                stat(7, data['lang'] + "pa1-")
                print_and_log ("<< 1 parse [date]: " + pdate_text + ' lang:[' + lang_text +'] : ' + title_text +' ---content : ' + content_text[:80])

                # educt?
                if need_educt:
                    item['edc'] = get_educt(options.educt, title_text + ' ' + content_text)
                    if item['edc']:
                        stat(7, "pedc1-")
                        stat(7, data['lang'] + "pedc1-")
                    else:
                        stat(7, "pedc0-")
                        stat(7, data['lang'] + "pedc0-")
                rsp = gen_suc_rsp(item)
            else:
                stat(7, "pa0-")
                stat(7, data['lang'] + "pa0-")
                print_and_log ("xx 1 error on parse [date]: " + pdate_text + ' lang:[' + lang_text +'] : ' + title_text +' ---content : ' + content_text[:80])
                rsp = gen_fail_rsp(item)
            self.set_header('Content-Type','application/json')
            rsp = json.dumps(rsp,ensure_ascii=False,indent=4)
            #rsp = tornado.escape.json_encode(rsp).decode('unicode_escape')
            self.write(rsp)
            self.finish()
            #print_and_log ("<< return : " + json.dumps(item, ensure_ascii=False, indent=2, sort_keys=True))
            #pslogger = psdblogger('x9.ddns.net',27001)
            #pslogger.proccess(item,data['url'],data['idate'],data['lang'])
        except Exception as e:
            import traceback
            import StringIO
            stat(7, "pe-")
            stat(7, data['lang'] + "pe-")
            self.set_header('Content-Type','application/json')
            self.write(gen_fail_rsp({'message':'error occur.'}))
            self.finish()

            fp = StringIO.StringIO()
            traceback.print_exc(file=fp)
            message = fp.getvalue()
            print_and_log( "=== parse service exception: ==== \n" + message)


class application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r'/parse',parseHandler),
            (r"/edc", edcHandler)
        ]
        tornado.web.Application.__init__(self,handlers)

def main():
    print_and_log("\n\n parser service starting ... ")
    tornado.options.parse_config_file("./config/service.cfg")
    tornado.options.parse_command_line()
    app = application()
    http_server = tornado.httpserver.HTTPServer(app)

    http_server.listen(options.port)
    print_and_log("\n listening on port %d ... " %(options.port))
    tornado.ioloop.IOLoop.instance().start()
    #tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        print(e)
