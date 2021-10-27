import shutil
from datetime import datetime
import os

FILE_TYPE = "csv"
CURRENT_DATE = datetime.today().strftime("%Y%m%d")
USER_DIR_NAME = ["jj", "mk", "sw"]


def copy_file(user_name):
    scala_output_path = f"./front/output/{user_name}/{CURRENT_DATE}"
    csv_file_list = [file for file in os.listdir(scala_output_path) if file.endswith(FILE_TYPE)]
    for(csv_file) in csv_file_list:
        result_file_name = f"{user_name}_{csv_file}"
        shutil.copyfile(f"./front/output/{user_name}/{CURRENT_DATE}/{csv_file}", f"./front/src/resources/{result_file_name}")



def main():
    for(user) in USER_DIR_NAME:
        copy_file(user)


if __name__ == '__main__':
    main()