# coding: utf-8
import pandas as pd
import csv
from os import read
import pprint

INPUT_PATH = './39KOCHI.CSV'


def read_csv():
    with open(INPUT_PATH, encoding="shift_jis") as f:
        print(f.read())


def pandas_csv():
    df = pd.read_csv(INPUT_PATH, encoding="shift_jis", header=None)
    # 住所名抽出
    print(df.iloc[:, 6:9])


def main():
    pandas_csv()

if __name__ == '__main__':
    main()