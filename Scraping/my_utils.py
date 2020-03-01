import json
import requests
from os import path

SCRAPED_DATA_PATH = "./scraped_data"


def json2dict(filename):
    filename = SCRAPED_DATA_PATH + "/" + filename
    if not path.exists(filename):
        initialize_empty_json(filename)
    try:
        with open(filename, 'r') as f:
            loaded_data = json.load(f)
    except IOError:
        print('Could not Open File: %s' % filename)

    return loaded_data


def anything2json(list, identifier):
    filename = ("%s/%s.json" % (SCRAPED_DATA_PATH, identifier))
    if not path.exists(filename):
        initialize_empty_json(filename)
    try:
        with open(filename, mode='w', encoding='utf-8') as f:
            json.dump(list, f, ensure_ascii=False, indent=4)
    except IOError:
        print('Could not Write to File: %s' % filename)


def dict2json3_live(dict, identifier="all_reports_all_years_all_pages"):
    filename = ("%s/%s.json" % (SCRAPED_DATA_PATH, identifier))
    if not path.exists(filename):
        initialize_empty_json(filename)
    try:
        with open(filename, encoding='utf-8') as f:
            data = json.load(f)
        data["items"].append(dict)
        with open(filename, 'w') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except IOError:
        print('Could not Write to File: %s' % filename)


def is_item_already_in_Database(url, db_identifier="all_reports_all_years_all_pages"):
    filename = ("%s/%s.json" % (SCRAPED_DATA_PATH, db_identifier))
    if not path.exists(filename):
        initialize_empty_json(filename)
    try:
        with open(filename, encoding='utf-8') as f:
            data = json.load(f)
    except IOError:
        print('Could not Open File: %s' % filename)

    for scraped_article in data["items"]:
        scraped_article_url = scraped_article["url"]
        scraped_article_code = scraped_article["response"]
        if scraped_article["url"] == url and scraped_article["response"] == 200:
            return True
    return False


def initialize_empty_json(filename):
    with open(filename, mode='w', encoding='utf-8') as f:
        json.dump({"items": []}, f, ensure_ascii=False, indent=4)
