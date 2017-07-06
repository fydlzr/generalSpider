useNewspaper = set([
	'www.vic.gov.au',	

])
xpaths = {
	'www.infrastructure.gov.au': {
		'lang':'en',
		'title':['//article//h1/text()'],
		'pdate':['//*[@id="text"]/div/div[1]/p[3]/text()'],
		'showcontent':['//*[@id="text"]//p/text()'],
	},
	'www.dfat.gov.au': {
                'lang':'en',
		'title':['//header/h1/text()'],
                'pdate':['//*[@id="text"]/div/div[1]/p[3]/text()'],
                'showcontent':['//div[@class="contentarea"]','//div[starts-with(@id,"WebPartZone")]/div/*'],
		'category':['//header/p/a/text()'],
        },
	'www.gov.uk': {
		'lang':'en',
                'title':['//header/h1/text()'],
                'attr':['//div[@class="govuk-metadata direction-ltr"]/dl'],
                'showcontent':['//*[@id="content"]/p','//*[@id="govspeak"]/*'],
        },
	'www.nsw.gov.au': {
                'lang':'en', 
                'title':['//p[starts-with(@class,"heading-3")]'],
                'pdate':['//div[@class="article-content__bottom-section-content"]/div/span[1]'],
                'showcontent':['//*[@class="element ElementContent"]/div/*'],
        },
	'www.birmingham.gov.uk': { 
                'lang':'en',
                'title':['//*[@id="content"]/h1'],
                'pdate':['//*[@id="content"]/div[1]/span'],
                'showcontent':['//*[@id="content"]/p', '//*[@id="content"]/div[2]'],
        },
	'www.londoncouncils.gov.uk': {
                'lang':'en',
                'title':['//h1[@class="page_header"]'],
                'pdate':['//*[@id="content"]/div[1]/span'],
                'showcontent':['//*[@id="content"]/p', '//*[@id="content"]/div[2]'],
        },
	'www.treasury.gov.my': {
                'lang':'en',
                #'title':['//h2[@class="itemTitle"]'],
                #'pdate':['//span[@class="itemDateCreated"]/text()'],
                'showcontent':['//div[@class="itemFullText"]'],
        },
}
