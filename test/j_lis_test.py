import pandas as pd
from pprint import pprint
from modules.elastic_control import Elasticsearch_modules
from time import time


INPUT_ADDRESS_CSV_PATH = 'data/j-lis/kochi/39_2020.csv'

# OUTPUT_GEOCODING_PATH = 'data/39KOCHI_GEO.CSV'


def pandas_address_csv():
    df = pd.read_csv(INPUT_ADDRESS_CSV_PATH, encoding="shift_jis")
    return df


def main():
    data = pandas_address_csv().values.tolist()
    elm = Elasticsearch_modules()
    insert_doc = []
    # start = time()
    for i, d in enumerate(data):
        if i < 100:
            tem = {
                "id": i,
                "prefectures_kanji": d[0],
                "municipalities_kanji": d[1],
                "town_area_kanji": d[2],
                "banchi": d[4],
                "point_location": f'{d[8], d[9]}',
            }
            insert_doc.append(tem)
        else:
            break
        # elm.insert_document("address", tem, id=i)
    elm.bulk_insert_document(index="j_lis", document=insert_doc)
    # elapsed_time = time() - start
    # print ("elapsed_time:{0}".format(elapsed_time) + "[sec]")


if __name__ == '__main__':
    main()