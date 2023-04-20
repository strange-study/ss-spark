import os
import sys
from datetime import datetime, time, timedelta

import requests
from bs4 import BeautifulSoup
from pytz import timezone

from time import sleep

START_DT = datetime.combine(datetime.now(timezone('Asia/Seoul')).date() - timedelta(days=1), time(6))

URL = 'https://gall.dcinside.com/board/lists/'
HEADERS = {'User-Agent': 'testt'}
MAX_INT = sys.maxsize
OUTPUT_DIR = "output"
INPUT_DIR = "output/board_ids.csv"

class Post:
    columns = ["c_id", "title", "view", "recommend", "comment_num", "date"]

    def __init__(self, num=None, title=None, view=None, recommend=None, comment_num=0, date=None):
        self.c_id = num
        self.title = title
        self.view = view
        self.recommend = recommend
        self.comment_num = comment_num
        self.date = date

    def get_header(self):
        return ",".join(Post.columns)

    def to_csv_row(self):
        return ",".join(map(str, [self.c_id, self.title, self.view, self.recommend, self.comment_num, self.date]))


# Get contents from Html (by css selector)
def get_contents_from_html(response):
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    contents = soup.find('tbody').find_all('tr')
    return contents


# Get post
def get_post(content, num, date):
    title = content.find('a').text.replace(",", " ") # replace ',' to blank
    view = int(content.find('td', class_='gall_count').text)
    recommend = int(content.find('td', class_='gall_recommend').text)

    # comment_num (Option)
    comment_num = 0
    comment = content.find('a', class_='reply_numbox')
    if comment:
        c = comment.text.strip('[]')
        comment_num = int(c) if c.isdigit() else 0

    return Post(num, title, view, recommend, comment_num, date)


def get_pages(board_id, output):
    is_today, prev_id = True, MAX_INT
    params = {'id': board_id, 'page': 0}
    retry = 3

    while is_today and retry > 0 :
        params['page'] += 1
        response = requests.get(URL, params=params, headers=HEADERS)
        print("scraping... page - {}".format(params['page']))

        if response.status_code != 200:
            print("[ERROR] RESPONSE CODE : {}".format(response.status_code))
            return
        try:
            contents = get_contents_from_html(response)

            for content in contents:
                num = content.find('td', class_='gall_num').text
                date_tag = content.find('td', class_='gall_date')

                if "운영자" in content.find('td', class_='gall_writer').text :
                    continue
                elif not num.isdigit() or int(num) >= prev_id:
                    continue
                elif not date_tag.has_attr('title') :
                    continue

                c_id = int(num)
                date = datetime.strptime(date_tag.attrs['title'], '%Y-%m-%d %H:%M:%S')

                if date < START_DT:
                    is_today = False
                    break

                try:
                    post = get_post(content, c_id, date)
                    # File Write
                    output.write(post.to_csv_row() + "\n")
                    prev_id = c_id

                except Exception as e:
                    print("skip exception : ", e)
                    print(content)
        except Exception as e:
            print("--------------------------")
            if retry :
                print("[ERROR] SKIP '{}' GALLARY".format(board_id))
                return
            else :
                print("[WARNING] FAILED '{}' GALLARY ({})".format(board_id, params['page']))
                print("[WARNING] SLEEP 10S AND RETRY (REMAIN RETRY COUNT : {}".foramt(retry))
                sleep(10)
                params['page'] -= 1
                retry -= 1

    print("--------------------------")
    print("SCRAPED PAGES : 1 ~ {}\n".format(params['page']))


def single(gall_name):
    start_time = datetime.now()
    today = start_time.strftime("%Y%m%d")

    output_path = f"{OUTPUT_DIR}/{today}/{gall_name}.csv"  # csv
    o = open(output_path, "w")
    print(f"Save to '{output_path}'")

    o.write(Post().get_header()+"\n")
    # get Today's Contents
    get_pages(gall_name, o)
    o.close()
    print("[END] TOTAL TIME : ", datetime.now() - start_time)

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
        o.write(Post().get_header() + "\n")
        # get Today's posts
        get_pages(gall_name, o)
        o.close()
    print("[END] TOTAL TIME : ", datetime.now() - start_time)


def print_warning_empty_borad_ids_file():
    print("[ERROR] 'board_ids.csv' file is required.\n\nusage: 'python3 dc_board_id_scraper.py' -> 'python3 dc_scraper.py'")


if __name__ == "__main__":
    if len(sys.argv) >= 2 :
        single(sys.argv[1])
    elif not os.path.exists(f"{INPUT_DIR}"):
        print_warning_empty_borad_ids_file()
    else:
        inputList = []
        i = open(INPUT_DIR, "r")
        for itr in i:
            inputList.append(itr.replace('\n', ''))
        main(inputList)
        i.close()
