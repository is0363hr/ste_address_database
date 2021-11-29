import pandas as pd
import googlemaps
from config.api import GEOCODING_API


class Geocoding_modules:
    def __init__(self, input_address_path, output_geocoding_path) -> None:
        self.input_address_path = input_address_path
        self.output_geocoding_path = output_geocoding_path


    def pandas_address_csv(self):
        df = pd.read_csv(self.input_address_path, encoding="shift_jis", header=None)
        # 住所名抽出
        address = df.iloc[:, 2:9].values.tolist()
        data = [i for i in address if i[6] != '以下に掲載がない場合']
        return data


    def geocoding(self, data):
        gmaps = googlemaps.Client(key=GEOCODING_API)
        result = gmaps.geocode(data)
        return result


    def address_gecoding(self, limit):
        data = self.pandas_address_csv()
        data = [d for i, d in enumerate(data) if i <= limit]
        header = [
            "zip_code", "prefectures_kana", "municipalities_kana", "town_area_kana",
            "prefectures_kanji", "municipalities_kanji", "town_area_kanji",
            "point_location", "area_location_northeast", "area_location_southwest"]

        for i, d in enumerate(data):
            search_address = d[4] + d[5] + d[6]
            geocoding_result = self.geocoding(search_address)
            lat = geocoding_result[0]["geometry"]["location"]["lat"]
            lng = geocoding_result[0]["geometry"]["location"]["lng"]
            location = f'{lat}, {lng}'
            northeast = f'{geocoding_result[0]["geometry"]["viewport"]["northeast"]["lat"]}, {geocoding_result[0]["geometry"]["viewport"]["northeast"]["lng"]}'
            southwest = f'{geocoding_result[0]["geometry"]["viewport"]["southwest"]["lat"]}, {geocoding_result[0]["geometry"]["viewport"]["southwest"]["lng"]}'
            data[i].extend([location, northeast, southwest])

        data_df = pd.DataFrame(data, columns=[header])
        data_df.to_csv(self.output_geocoding_path, encoding="shift-jis")


def main():
    INPUT_ADDRESS_PATH = 'data/39KOCHI.CSV'
    OUTPUT_GEOCODING_PATH = 'data/39KOCHI_GEO.CSV'
    LIMIT = 100

    gcm = Geocoding_modules(INPUT_ADDRESS_PATH, OUTPUT_GEOCODING_PATH)
    gcm.address_gecoding(LIMIT)


if __name__ == '__main__':
    main()