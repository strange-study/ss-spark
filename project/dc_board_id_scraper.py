import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup

BASE_URL = 'https://m.dcinside.com/category/hotgall'
OUTPUT_DIR = "output"
FILE_NAME = "board_ids"
FILE_TYPE = "csv"


def write_board_ids(out_stream):
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        hot_gallery_list = soup.find('section').find_all('li')
        for idx in range(len(hot_gallery_list)):
            board_url = hot_gallery_list[idx].find('a')['href']
            board_id = board_url.replace('https://m.dcinside.com/board/', '')
            out_stream.write(board_id + "\n")
    else:
        print(response.status_code)


def main():
    start_time = datetime.now()
    if not os.path.exists(f"{OUTPUT_DIR}"):
        os.makedirs(f"{OUTPUT_DIR}")
    output_path = f"{OUTPUT_DIR}/{FILE_NAME}.{FILE_TYPE}"
    o = open(output_path, "w")
    print(f"Save to '{output_path}'")
    write_board_ids(o)
    o.close()
    print("[END] TOTAL TIME : ", datetime.now() - start_time)


if __name__ == '__main__':
    main()
