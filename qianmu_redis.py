import threading
import requests
import time
from lxml import etree
import redis
import signal
import scrapy

START_URL = 'http://qianmu.iguye.com/2018USNEWS世界大学排名'
threads = []
DOWNLOADER_NUM = 10
download_pages = 0
r = redis.Redis()
threads_on = True
DOWNLOADER_DELAY = 0.1

def fetch(url,raise_err=False):
    global download_pages
    # print(url)
    try:
        r = requests.get(url)
    except Exception as e:
        print(e)
    else:
        download_pages += 1
        if raise_err:
            r.raise_for_status()
    return r.text.replace('\t','')

def parse(html):
    # global link_queue
    selector = etree.HTML(html)
    links = selector.xpath('//*[@id="content"]/table/tbody/tr/td[2]/a/@href')
    # link_queue += links
    # return links
    for link in links:
        if not link.startswith('http://'):
            link = 'http://qianmu.iguye.com/{}'.format(link)
        # link_queue.put(link)
        if r.sadd('qianmu.seen',link):
            r.lpush('qianmu.queue',link)


def parse_university(html):
    selector = etree.HTML(html)
    table = selector.xpath('//*[@id="wikiContent"]/div[1]/table/tbody')
    if not table:
        return
    table = table[0]
    keys = table.xpath('./tr/td[1]//text()')
    cols = table.xpath('./tr/td[2]')
    values = [''.join(col.xpath('.//text()')) for col in cols]
    # values = table.xpath('./tr/td[2]//text()')
    info = dict(zip(keys, values))
    # print(info)
    r.lpush('qianmu.items',info)

def downloader(i):
    print('Thread-{} start'.format(i))
    while threads_on:
        # link = link_queue.get()
        link = r.rpop('qianmu.queue')
        if link:
            link = link.decode('utf-8')
            parse_university(fetch(link))
            print('Thread-{} remaiing queue:{}'.format(i,r.llen('qianmu.queue')))
        time.sleep(DOWNLOADER_DELAY)
        # print('----------remaining queue：{}'.format(link_queue.qsize()))
    print('Thread-{} exit now.'.format(i))

def sigint_handler(signum, frame):
    print('Received Ctrl + C,wait for gracefully')
    global threads_on
    threads_on = False


if __name__ == '__main__':
    start_time = time.time()
    parse(fetch(START_URL,raise_err=True))

    # 创建 10 个线程
    for i in range(DOWNLOADER_NUM):
        t = threading.Thread(target=downloader,args=(i+1,))
        t.start()
        threads.append(t)

    signal.signal(signal.SIGINT,sigint_handler)

    for t in threads:
        t.join()

    cost_seconds = time.time() - start_time
    print('download {} pages,cost {} secods'.format(download_pages,cost_seconds))


