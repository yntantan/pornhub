#!/usr/bin/env python

import os
import json
import re

import requests
import threading

from time import sleep
from lxml import etree
from loguru import logger
from queue import PriorityQueue, Queue
from functools import total_ordering

logger.add("logs/%s.log" % __file__.rstrip('.py'), format="{time:MM-DD HH:mm:ss} {level} {message}")

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
}

proxies = {
    "http": "socks5://127.0.0.1:1080",
    "https": "socks5://127.0.0.1:1080",
}




class Producer:
    def __init__(self, q, urls, try_times=10):
        self.q = q
        self.urls = urls
        self.try_times = try_times
    
    def produce(self):
        for url in urls:
            self._list_page(url)
        self.q.join()
        
    def _list_page(self, url):
        logger.info('list_page: %s' % url)
        current_time = 0
        while current_time < self.try_times:
            try:
                resp = requests.get(url, headers=headers, proxies=proxies)
                break
            except Exception as err:
                logger.error('list page failed!\n{}'.format(err))
                sleep(3)
                current_time += 1
        if current_time == self.try_times:
            logger.error('try max times')
            return 
        html = etree.HTML(resp.content.decode('utf-8'))
        vkeys = html.xpath('//*[@class="phimage"]/div/a/@href')
        for i in range(len(vkeys)):
            vkey = vkeys[i].split('=')[-1]
            if 'ph' in vkey:
                u = "https://www.pornhub.com/view_video.php?viewkey={}".format(vkey) 
                self.q.put(u)


@total_ordering
class PriBuffer:
    def __init__(self, buffer_len):
        self.bu_len = buffer_len
        self.q = Queue(buffer_len)

    def __lt__(self, other):
        return self.q.qsize() < other.q.qsize()

    def __eq__(self, other):
        return self.q.qsize() == ohter.q.qsize()

    def put(self, arg):
        self.q.put(arg)

    def get(self):
        return self.q.get()


class Downloader:
    def __init__(self, q, num=20, buffer_len=4, try_times=10):
        self.q = q
        self.num = num
        self.workers_buffer = list()
        self.scheduler = PriorityQueue(num)
        self.max_times = try_times
        for i in range(self.num):
            self.workers_buffer.append(PriBuffer(buffer_len))
            self.scheduler.put(self.workers_buffer[i])

    def start(self):
        threading.Thread(target=self._schedule, daemon=True).start()
        for i in range(self.num):
            threading.Thread(target=self._download, args=(i,), daemon=True).start()

    def _schedule(self):
        while True:
            url = self.q.get()
            logger.info('schedule:' + url)
            if url is None:
                break
            pb = self.scheduler.get()
            pb.put(url)
            self.scheduler.put(pb)

    def _download(self, qid):
        while True:
            url = self.workers_buffer[qid].get()
            if url is None:
                break
            self._detail_page(url)
            self.q.task_done()

    def _detail_page(self, url):
        s = requests.Session()
        current_time = 0
        while current_time < self.max_times:
            try:
                resp = s.get(url, headers=headers, proxies=proxies)
                break
            except Exception as err:
                logger.error('detail page retry {} {}'.format(url, current_time))
                sleep(3)
                current_time += 1
        if current_time == self.max_times:
            logger.error('detail page max retry times')
            return
        html = etree.HTML(resp.content.decode('utf-8'))
        title = '_'.join(''.join(html.xpath('//h1//text()')).strip().split(' ')).replace('/','')
        logger.info('download: {}'.format(title))

        js = html.xpath('//*[@id="player"]/script/text()')[0]
        tem = re.findall('var\\s+\\w+\\s+=\\s+(.*);\\s+var player_mp4_seek', js)[-1]
        con = json.loads(tem)

        for _dict in con['mediaDefinitions']:
            if 'quality' in _dict.keys() and _dict.get('videoUrl'):
                try:
                    self._download_video(_dict.get('videoUrl'), title, 'mp4')
                    break 
                except Exception as err:
                    logger.error(err)

    def _download_video(self, url, name, filetype):
        filepath = '%s/%s.%s' % (filetype, name, filetype)
        current_bytes = 0 
        current_time = 0
        with requests.Session() as s: 
            while True: 
                s.headers['Range'] = 'bytes={}-'.format(current_bytes) 
                with open(filepath, 'ab') as f: 
                    while current_time < self.max_times:
                        try:
                            r = s.get(url, stream=True, headers=headers, proxies=proxies) 
                            f.seek(current_bytes)
                            for chunk in r.iter_content(chunk_size=8192):  
                                if chunk: # filter out keep-alive new chunks 
                                    current_bytes += len(chunk) 
                                    f.write(chunk) 
                                    f.flush() 
                            break
                        except Exception as err:
                            logger.error('download {}:{} try max times {}'.format(url, name, current_time))
                            sleep(3)
                            current_time += 1

                    if current_time == self.max_times:
                        break
                content_range = r.headers['Content-Range'] 
                if content_range == '' or content_range is None: 
                    logger.warn('download: {} try max times {} content range is none'.format(name, current_time))
                    break 
                total_size = int(content_range.split('/')[1]) 
                logger.info('download {} total size: {}  load size: {}'.format(name, total_size, current_bytes)) 
                if total_size == current_bytes: 
                    break 

if __name__ == '__main__':
    init_url = 'https://www.pornhub.com/video?page={}'
    urls = list()
    for i in range(1, 50):
        urls.append(init_url.format(i))
    q = Queue()
    Downloader(q).start()
    Producer(q, urls).produce()



