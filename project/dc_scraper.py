import os
import sys
from datetime import datetime, time, timedelta

import requests
from bs4 import BeautifulSoup
from pytz import timezone


class Post:
    def __init__(self, num=None, title=None, view=None, recommend=None, comment_num=0, date=None):
        self.c_id = num
        self.title = title
        self.view = view
        self.recommend = recommend
        self.comment_num = comment_num
        self.date = date

    def getHeader(self):
        return ",".join(["c_id", "title", "view", "recommend", "comment_num", "date"])

    def toCsvRow(self):
        return ",".join(map(str, [self.c_id, self.title, self.view, self.recommend, self.comment_num, self.date]))


START_DT = datetime.combine(datetime.now(timezone('Asia/Seoul')).date() - timedelta(days=1), time(6))

# mobile URL : https://m.dcinside.com/board/${id}
URL = 'https://gall.dcinside.com/board/lists/'
HEADERS = {'User-Agent': 'test'}
MAX_INT = sys.maxsize
OUTPUT_DIR = "output"
INPUT_DIR = "output/board_ids.csv"


# Get contents from Html (by css selector)
def getContentsFromHtml(response):
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    contents = soup.find('tbody').find_all('tr')
    return contents


# 
def getPost(content, num):
    date_tag = content.find('td', class_='gall_date')
    date = datetime.strptime(date_tag.attrs['title'], '%Y-%m-%d %H:%M:%S')
    # not tdday
    if date < START_DT:
        return None

    title = content.find('a').text
    view = int(content.find('td', class_='gall_count').text)
    recommend = int(content.find('td', class_='gall_recommend').text)

    # comment_num (Option)
    comment_num = 0
    comment = content.find('a', class_='reply_numbox')
    if comment:
        c = comment.text.strip('[]')
        comment_num = int(c) if c.isdigit() else 0

    return Post(num, title, view, recommend, comment_num, date)


def getPages(board_id, o):
    is_today, prev_id = True, MAX_INT
    params = {'id': board_id, 'page': 0}

    while is_today:
        params['page'] += 1
        response = requests.get(URL, params=params, headers=HEADERS)
        print("scraping... page - {}".format(params['page']))

        if response.status_code != 200:
            print("[ERROR] RESPONSE CODE : {}".format(response.status_code))
            return

        contents = getContentsFromHtml(response)
        for i in contents:
            num = i.find('td', class_='gall_num').text
            if not num.isdigit() or int(num) >= prev_id:
                continue

            c_id = int(num)
            post = getPost(i, c_id)

            if not post:
                is_today = False
                break

            # File Write
            o.write(post.toCsvRow() + "\n")
            prev_id = c_id

    print("--------------------------")
    print("SCRAPED PAGES : 1 ~ {}".format(params['page']))


def main(gall_names):
    start_time = datetime.now()
    today = start_time.strftime("%Y%m%d")
    if not os.path.exists(f"{OUTPUT_DIR}/{today}"):
        os.makedirs(f"{OUTPUT_DIR}/{today}")

    for gall_name in gall_names:
        output_path = f"{OUTPUT_DIR}/{today}/{gall_name}.csv"  # csv
        o = open(output_path, "w")
        print(f"Save to '{output_path}'")

        # Write Data Header
        o.write(Post().getHeader() + "\n")
        # get Today's posts
        getPages(gall_name, o)
        o.close()
    print("[END] TOTAL TIME : ", datetime.now() - start_time)


def printWarningEmptyBoardIdsFile():
    print(
        "[ERROR] 'board_ids.csv' file is required.\n\nusage: 'python3 dc_board_id_scraper.py' -> 'python3 dc_scraper.py'")


if __name__ == "__main__":
    if not os.path.exists(f"{INPUT_DIR}"):
        printWarningEmptyBoardIdsFile()
    else:
        inputList = []
        i = open(INPUT_DIR, "r")
        for itr in i:
            inputList.append(itr.replace('\n', ''))
        main(inputList)
