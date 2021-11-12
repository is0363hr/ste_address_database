import pandas as pd
import requests
import io
import zipfile


def file_download(url):
    r = requests.get(url).content

    output_path = url.split('/')[-1].split('.')[0] + ".zip"
    # wb でバイト型を書き込める
    with open(output_path, mode='wb') as f:
        f.write(r)

    return output_path


def unzip(path):
    with zipfile.ZipFile(path) as existing_zip:
        # print(existing_zip.namelist())
        existing_zip.extractall()


def main():
    url = "https://www.post.japanpost.jp/zipcode/dl/oogaki/zip/01hokkai.zip"
    path = url.split('/')[-1].split('.')[0] + ".zip"
    # path = file_download(url)
    unzip(path)


if __name__ == '__main__':
    main()
