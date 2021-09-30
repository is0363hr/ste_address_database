# coding: utf-8
import pandas as pd
import csv
from os import read
from pprint import pprint
from config.api import GEOCODING_API
import googlemaps

INPUT_PATH = './39KOCHI.CSV'


def read_csv():
    with open(INPUT_PATH, encoding="shift_jis") as f:
        print(f.read())


def pandas_csv():
    df = pd.read_csv(INPUT_PATH, encoding="shift_jis", header=None)
    # 住所名抽出
    address = df.iloc[:, 6:9].values.tolist()
    # for add in address:
    #     if add[2] == '以下に掲載がない場合':
    #         continue
    #     else:
    #         print(add)
    return address


def geocoding(data):
    gmaps = googlemaps.Client(key=GEOCODING_API)
    result = gmaps.geocode(data)
    return result


def main():
    data = pandas_csv()
    sample = data[1][0] + data[1][1] + data[1][2]
    print(sample)
    result = geocoding(sample)
    lat = result[0]["geometry"]["location"]["lat"]
    lng = result[0]["geometry"]["location"]["lng"]
    pprint(result)
    print(lat, lng)


if __name__ == '__main__':
    main()