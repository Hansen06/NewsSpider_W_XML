
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
# 运行一个
process = CrawlerProcess(get_project_settings())


process.crawl('uyghur_people_news')
process.crawl('uyghur_cctv')

process.start()
