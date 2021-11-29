# coding: utf-8
import pandas as pd
from pprint import pprint
from modules.elastic_control import Elasticsearch_modules
from time import time


INPUT_GEOCODING_PATH = 'data/39KOCHI_GEO.CSV'
# OUTPUT_GEOCODING_PATH = 'data/39KOCHI_GEO.CSV'


def pandas_address_csv():
    df = pd.read_csv(INPUT_GEOCODING_PATH, encoding="shift_jis")
    return df


def main():
    data = pandas_address_csv().values.tolist()
    elm = Elasticsearch_modules()
    insert_doc = []
    start = time()
    for i, d in enumerate(data):
        tem = {
            "id": d[0],
            "zip_code": d[1],
            "prefectures_kana": d[2],
            "municipalities_kana": d[3],
            "town_area_kana": d[4],
            "prefectures_kanji": d[5],
            "municipalities_kanji": d[6],
            "town_area_kanji": d[7],
            "point_location": d[8],
            "area_location": {
                    "northeast": d[9],
                    "southwest": d[10],
            },
        }
        insert_doc.append(tem)
        # elm.insert_document("address", tem, id=i)
    elm.bulk_insert_document(index="address", document=insert_doc)
    elapsed_time = time() - start
    print ("elapsed_time:{0}".format(elapsed_time) + "[sec]")


if __name__ == '__main__':
    main()