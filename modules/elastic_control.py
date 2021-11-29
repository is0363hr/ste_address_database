from elasticsearch import Elasticsearch, helpers
from config.api import RASPBERRY_PI4_IP_ADDRESS
from pprint import pprint


# -------- const --------
PORT = 9200
USER_ID = "id"
PASSWORD = "passwd"
isDebug = True
# -----------------------


class Elasticsearch_modules:
    def __init__(self) -> None:
        self.es = Elasticsearch(
            # ["localhost", "otherhost"],
            "http://" + RASPBERRY_PI4_IP_ADDRESS,
            scheme="http",
            port=PORT
            # http_auth=(USER_ID, PASSWORD)
        )


    def disconnect(self):
        self.es.close()


    def create_data(self, index, mapping):
        self.es.indices.create(index=index, body=mapping)


    def insert_document(self, index, document, id=None):
        # ドキュメントの登録
        if id == None:
            self.es.create(index=index, body=document)
        else:
            self.es.create(index=index, id=id, body=document)


    def update_document(self, index, document, id):
        # ドキュメントの登録
        self.es.update(index=index, id=id, body=document)


    def convert_document(self, index, document):
        # bulkで扱えるデータ構造に変換
        for doc in document:
            yield {
                "_op_type": "create",
                "_index": index,
                "_source": doc
            }


    def bulk_insert_document(self, index, document):
        # ドキュメントを一括で登録
        helpers.bulk(self.es, self.convert_document(index, document))


    def get_index(self):
        # インデックス一覧の取得
        return self.es.cat.indices(index='*', h='index').splitlines()


    def get_mapping(self, index):
        return self.es.indices.get_mapping(index=index)


    def update_mapping(self, index, mapping):
        self.es.indices.put_mapping(index=index, body=mapping)


    def delete_index(self, index):
        self.es.indices.delete(index=index)


    def search_all(self, index):
        # ドキュメントを検索
        result = self.es.search(index=index)
        # 検索結果からドキュメントの内容のみ表示
        for document in result["hits"]["hits"]:
            yield { document["_source"] }


    def search_id(self, index, id):
        return self.es.get_source(index=index, id=id)


    def search_query(self, index, size=None):
        # ドキュメントを検索
        if size == None:
            result = self.es.search(index=index, body=query)
        else:
            result = self.es.search(index=index, body=query, size=size)
        for document in result["hits"]["hits"]:
            yield { document["_source"] }


    def search_count(self, index):
        return self.es.count(index=index)


    def address_doc_reset(self):
        mapping = {
            "mappings": {
                "properties": {
                    "zip_code": {"type": "text"},
                    "prefectures_kana": {"type": "text"},
                    "municipalities_kana": {"type": "text"},
                    "town_area_kana": {"type": "text"},
                    "prefectures_kanji": {"type": "text"},
                    "municipalities_kanji": {"type": "text"},
                    "town_area_kanji": {"type": "text"},
                    "point_location": {"type": "geo_point"},
                    "area_location": {
                        "properties": {
                            "northeast": {"type": "geo_point"},
                            "southwest": {"type": "geo_point"},
                        },
                    }
                }
            }
        }
        index = "address"
        self.delete_index(index)
        self.create_data(index, mapping)
        pprint(self.get_mapping(index))


    def j_lis_doc_reset(self):
        mapping = {
            "mappings": {
                "properties": {
                    "prefectures_kanji": {"type": "text"},
                    "municipalities_kanji": {"type": "text"},
                    "town_area_kanji": {"type": "text"},
                    "banchi": {"type": "text"},
                    "point_location": {"type": "geo_point"},
                }
            }
        }
        index = "j_lis"
        self.delete_index(index)
        self.create_data(index, mapping)
        pprint(self.get_mapping(index))



def main():
    elm = Elasticsearch_modules()
    # elm.address_doc_reset()
    elm.j_lis_doc_reset()


if __name__ == '__main__':
    main()