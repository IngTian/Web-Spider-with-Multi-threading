# Local imports.
from time import sleep

from spider.retry import Retry

# Util imports.
from enum import Enum, unique
from bson import Binary
from hashlib import sha1
from bs4 import BeautifulSoup

# Multi-threading.
from threading import Thread, current_thread, local

# Tools related to persistence.
import pymongo
import redis
import pickle
import zlib

# Web-related tools.
import requests
from urllib.parse import urlparse

thread_local = local()
hash_protocol = sha1()


@unique
class SpiderStatus(Enum):
    IDLE = 0
    WORKING = 1


def decode_page(page_bytes, charsets=('utf-8', 'ascii', 'utf-16')):
    """
    Decode bytes-form web page into a web page object.
    :param page_bytes: Web pages encoded in bytes.
    :param charsets: Decoding character sets.
    :return: The decoded web page.
    """
    page_html = None
    for charset in charsets:
        try:
            page_html = page_bytes.decode(charset)
            break
        except UnicodeDecodeError:
            pass
    return page_html


class Spider(object):

    def __init__(self, task_name="a_task"):
        self.status = SpiderStatus.IDLE
        self.__task_name = task_name

    @Retry()
    def fetch(self, current_url, *, charsets=('utf-8',),
              user_agent=None, proxies=None):
        """
        Download a web page.
        :param current_url: The URL of the web page.
        :param charsets: The character sets used to decode the page.
        :param user_agent: Mozilla?
        :param proxies: Any proxy overseas?
        :return: A web page object.
        """
        thread_name = current_thread().name
        print(f'[{thread_name}]: {current_url}')
        headers = {'user-agent': user_agent} if user_agent else {}
        resp = requests.get(current_url,
                            headers=headers, proxies=proxies)
        return decode_page(resp.content, charsets) \
            if resp.status_code == 200 else None

    def parse(self, html_page, *, domain='m.sohu.com'):
        """
        Extract more URLs from the web page
        and save them into Redis database.
        :param html_page: An HTML web page.
        :param domain: The web domain, e.g., mcgill.ca
        :return: Nothing.
        """
        soup = BeautifulSoup(html_page, 'lxml')
        for a_tag in soup.body.select('a[href]'):
            parser = urlparse(a_tag.attrs['href'])
            scheme = parser.scheme or 'http'
            netloc = parser.netloc or domain
            if scheme != 'javascript' and netloc == domain:
                path = parser.path
                query = '?' + parser.query if parser.query else ''
                full_url = f'{scheme}://{netloc}{path}{query}'
                redis_client = thread_local.redis_client
                if not redis_client.sismember('visited_urls', full_url):
                    redis_client.rpush(self.__task_name, full_url)

    def extract(self, html_page):
        # You can do something here. Guten Tag!.
        pass

    def store(self, data_dict):
        # Persistence layer.
        pass


class SpiderThread(Thread):

    def __init__(
            self,
            name,
            spider,
            redis_host="",
            redis_port=6379,
            redis_password="",
            mongo_host="",
            mongo_port=27017,
            task_name="a_task"
    ):
        super().__init__(name=name, daemon=True)
        self.__task_name = task_name
        self.spider = spider
        self.__redis_host = redis_host
        self.__redis_port = redis_port
        self.__redis_password = redis_password
        self.__mongo_host = mongo_host
        self.__mongo_port = mongo_port

    def run(self):
        """
        Start off the thread.
        Continuously downloading and fetching new web pages.
        :return:
        """
        redis_client = redis.Redis(host=self.__redis_host, port=self.__redis_port, password=self.__redis_password)
        mongo_client = pymongo.MongoClient(host=self.__mongo_host, port=self.__mongo_port)
        thread_local.redis_client = redis_client
        thread_local.mongo_db = mongo_client.msohu
        while True:
            current_url = redis_client.lpop(self.__task_name)
            while not current_url:
                current_url = redis_client.lpop(self.__task_name)
            self.spider.status = SpiderStatus.WORKING
            current_url = current_url.decode('utf-8')
            if not redis_client.sismember('visited_urls', current_url):
                redis_client.sadd('visited_urls', current_url)
                html_page = self.spider.fetch(current_url)
                if html_page not in [None, '']:
                    decoder = hash_protocol.copy()
                    decoder.update(current_url.encode('utf-8'))
                    doc_id = decoder.hexdigest()
                    document_data_coll = mongo_client.msohu.webpages
                    if not document_data_coll.find_one({'_id': doc_id}):
                        document_data_coll.insert_one({
                            '_id': doc_id,
                            'url': current_url,
                            'page': Binary(zlib.compress(pickle.dumps(html_page)))
                        })
                    self.spider.parse(html_page)
            self.spider.status = SpiderStatus.IDLE


def is_any_alive(spider_threads):
    """
    Check if any thread is still working.
    :param spider_threads: Some threads.
    :return: True for one or more threads are working, and false otherwise.
    """
    return any([spider_thread.spider.status == SpiderStatus.WORKING
                for spider_thread in spider_threads])


class SpiderThreadController:

    def __init__(self,
                 task_name="a_task",
                 redis_host="",
                 redis_port=6379,
                 redis_password="",
                 mongo_host="",
                 mongo_port=27017,
                 source_url="",
                 thread_number=10
                 ):
        self.__task_name = task_name
        self.__redis_host = redis_host
        self.__redis_port = redis_port
        self.__redis_password = redis_password
        self.__mongo_host = mongo_host
        self.__mongo_port = mongo_port
        self.__source_url = source_url
        self.__thread_number = thread_number

    def multi_thread_download(self):

        database_connect = redis.Redis(
            host=self.__redis_host,
            port=self.__redis_port,
            password=self.__redis_password
        )
        if not database_connect.exists(self.__task_name):
            database_connect.rpush(self.__task_name, self.__source_url)

        spider_threads = [SpiderThread(
            "spider_thread-%d" % i,
            Spider(),
            redis_host=self.__redis_host,
            redis_port=self.__redis_port,
            redis_password=self.__redis_password,
            mongo_host=self.__mongo_host,
            mongo_port=self.__mongo_port,
            task_name=self.__task_name
        ) for i in range(self.__thread_number)]

        for spider_thread in spider_threads:
            spider_thread.start()

        while database_connect.exists('a_task') or is_any_alive(spider_threads):
            sleep(5)

        return
