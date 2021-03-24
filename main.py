# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from spider.spider_thread import SpiderThreadController


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    controller = SpiderThreadController(
        task_name="something",
        redis_host="hello.world",
        redis_port=6379,
        redis_password="aPassword",
        mongo_host="guten.tag",
        mongo_port=27017,
        source_url="https://baidu.com",
        thread_number=10
    )
    controller.multi_thread_download()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
