# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from time import sleep

import redis
from spider.spider_thread import SpiderThread, Spider, is_any_alive


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def main():
    """
    A demo program.
    :return:
    """
    redis_client = redis.Redis(host='1.2.3.4', port=6379, password='hello')
    if not redis_client.exists('a_task'):
        redis_client.rpush('a_task', 'http://baidu.com')

    spider_threads = [SpiderThread('thread-%d' % i, Spider())
                      for i in range(10)]
    for spider_thread in spider_threads:
        spider_thread.start()

    while redis_client.exists('a_task') or is_any_alive(spider_threads):
        sleep(5)

    print('Done')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
