# coding: utf-8
import pandas as pd
from os import read
from pprint import pprint
from config.api import GEOCODING_API
import googlemaps
from modules.elastic_control import Elasticsearch_modules

INPUT_PATH = 'data/39KOCHI.CSV'


def read_csv():
    with open(INPUT_PATH, encoding="shift_jis") as f:
        print(f.read())


def pandas_csv():
    df = pd.read_csv(INPUT_PATH, encoding="shift_jis", header=None)
    # 住所名抽出
    address = df.iloc[:, 2:9].values.tolist()
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
    _input = pandas_csv()
    data = [i for i in _input if i[6] != '以下に掲載がない場合']

    elm = Elasticsearch_modules()

    data = [d for i, d in enumerate(data) if i <= 10]
    insert_doc = []
    for i, d in enumerate(data):
        search_address = d[4] + d[5] + d[6]
        geocoding_result = geocoding(search_address)
        lat = geocoding_result[0]["geometry"]["location"]["lat"]
        lng = geocoding_result[0]["geometry"]["location"]["lng"]
        northeast = [geocoding_result[0]["geometry"]["viewport"]["northeast"]["lat"], geocoding_result[0]["geometry"]["viewport"]["northeast"]["lng"]]
        southwest = [geocoding_result[0]["geometry"]["viewport"]["southwest"]["lat"], geocoding_result[0]["geometry"]["viewport"]["southwest"]["lng"]]
        data[i].extend([[lat, lng], northeast, southwest])
        tem = {
            "zip_code": d[0],
            "prefectures_kana": d[1],
            "municipalities_kana": d[2],
            "town_area_kana": d[3],
            "prefectures_kanji": d[4],
            "municipalities_kanji": d[5],
            "town_area_kanji": d[6],
            "point_location": d[7],
            "area_location": {
                    "northeast": d[8],
                    "southwest": d[9],
            },
        }
        pprint(tem)
        # insert_doc.append(tem)
        elm.insert_document("address", tem, id=i)

    pprint(insert_doc)
    # elm.bulk_insert_document(index="address", document=insert_doc)

    # sample = data[1][0] + data[1][1] + data[1][2]
    # print(sample)
    # result = geocoding(sample)
    # lat = result[0]["geometry"]["location"]["lat"]
    # lng = result[0]["geometry"]["location"]["lng"]
    # pprint(result)
    # print(lat, lng)


if __name__ == '__main__':
    main()