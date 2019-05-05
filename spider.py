import re
import requests
from lxml import etree
from gevent import monkey, pool
from openpyxl import Workbook
monkey.patch_all()


class TaskFile(object):
    def __call__(self, values):
        for value in values:
            if value != '' and value is not None:
                return value
        return ' '


class JobSpider(object):
    """
    51Job
    """
    def __init__(self, key, start, end):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                          ' (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
        }
        self.key = key
        self.start = start
        self.end = end
        self.task = TaskFile()
        self.pool = pool.Pool(20)
        self.education_list = ['高中', '大专', '本科', '硕士', '博士']
        self.wb = Workbook()
        self.ws = self.wb.active
        self.ws.append(['职位名称', '薪资', '地点', '经验', '学历', '发布时间', '职位信息', '公司名称', '公司规模', '公司类型'])

    def search_job(self, key, page):
        url = 'https://search.51job.com/list/000000,000000,0000,00,9,99,{},2,{}.html'.format(key, page)
        response = requests.get(url, headers=self.headers)
        html = etree.HTML(response.content)
        items = html.xpath('//div[@class="el"]/p/span/a/@href')
        for link in items:
            yield link

    def get_info(self, url):
        print(url)
        response = requests.get(url, headers=self.headers)
        html = etree.HTML(response.content)

        name = self.task(html.xpath('//div[@class="cn"]/h1/@title'))
        price = self.task(html.xpath('//div[@class="cn"]/strong/text()'))
        infos = self.task(html.xpath('//p[@class="msg ltype"]/@title')).replace('\xa0', '')

        address = self.task(infos.split('|'))
        experience = self.task(re.findall(r'\|(.+?经验)', infos))

        education = '无'
        for edu in self.education_list:
            if edu in infos:
                education = edu
                break

        f_time = self.task(re.findall(r'\|([\d-]+发布)', infos))

        job_info = ''.join(html.xpath('//div[@class="bmsg job_msg inbox"]//text()'))
        job_info = re.sub('\s', '', job_info)
        company_name = self.task(html.xpath('//div[@class="com_msg"]/a/p/@title'))
        company_size = self.task(html.xpath('//div[@class="com_tag"]/p[contains(text(),"人")]/@title'))
        company_type = self.task(html.xpath('//div[@class="com_tag"]/p[last()]/@title'))
        line = [name, price, address, experience, education, f_time, job_info, company_name, company_size, company_type]
        print(line)
        self.ws.append(line)

    def run(self):
        for page in range(self.start, self.end + 1):
            print('---------------{}------------------'.format(page))
            links = self.search_job(self.key, page)
            for link in links:
                # self.get_info(link)
                self.pool.spawn(self.get_info, link)
            self.pool.join()
            self.wb.save('job_info_1.xlsx')


if __name__ == '__main__':

    spider = JobSpider('数据分析师', 1, 2)
    spider.run()
