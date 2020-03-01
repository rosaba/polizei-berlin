import requests
import lxml.html
from my_utils import *
from ratelimit import limits, sleep_and_retry
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s",
    datefmt='%Y.%m.%d %H:%M:%S',
    filename="./sraping.log"
)

ARCHIVE_URL = "https://www.berlin.de/polizei/polizeimeldungen/archiv/"
BERLIN_URL = "https://www.berlin.de"
REFRESH_STATIC_DATA = False
LIVE = True
SCRAPED_DATA_PATH = "./scraped_data"
ONE_HOUR = 3600
ONE_MINUTE = 60
FOLLOW_LINK_RATE = 5
HEADERS = {
    'User-Agent': 'htw-studi-agent (http://htw.info-miner.de)',
    'From': 'thomas.hoppe@htw-berlin.de'
}


logging.info('-------------- Start of Crawling --------------')
logging.info('Crawling-Mode Refresh Static Data = {}'.format(REFRESH_STATIC_DATA))
logging.info('Crawling-Mode Live = {}'.format(LIVE))


@limits(calls=FOLLOW_LINK_RATE, period=1)
def get_archives():
    if REFRESH_STATIC_DATA:
        response = requests.get(ARCHIVE_URL, headers = HEADERS)
        doc = lxml.html.fromstring(response.content)
        links_to_archives = doc.xpath(
            "//div[@class = 'html5-section article']//div[@class = 'html5-section block modul-text_bild']/div[@class = 'html5-section body']//div[@class = 'textile']//a/@href")
        anything2json(links_to_archives, "archives_by_year")
    else:
        links_to_archives = json2dict("archives_by_year.json")

    logging.info("Succesfully loaded Archive URLs")
    return links_to_archives


@limits(calls=FOLLOW_LINK_RATE, period=1)
@sleep_and_retry
def get_links_per_archive_pages(archive_year):
    """
    Nehme ein einzelnen Archiv-Jahr, und gebe die Seitenlinks zurück, die Reports enthalten
    Beispiel: Im Jahr 2015 gibt es 47 Seiten - Es wird eine Liste mit 47 Seiten zurückgegeben
    :param archive_year: Das Archiv Jahr, das durchsucht wird
    :return: Eine Liste mit allen Links zu Seiten die Reports enthalten in einem bestimmten Archiv-Jahr
    """
    url = BERLIN_URL + archive_year
    response = requests.get(url, headers = HEADERS)

    if response.status_code != 200:
        logging.error(
            'get_links_to_reports(single_archive_page) - response: {} - url {}'.format(response.status_code, url))
        # Throw Exception, that is caught by decorater, will Retry later!
        #raise Exception('API response: {}'.format(response.status_code))

    doc = lxml.html.fromstring(response.content)
    last_site = doc.xpath("//li[@class = 'pager-item last']/a/text()")[0]
    links_per_archive = []
    for i in range(1, int(last_site) + 1):
        lin = archive_year + "?page_at_1_0=" + str(i)
        links_per_archive.append(lin)

    logging.info("Succesfully generated catalogue for all pages-links for the year: {}".format(archive_year))
    return links_per_archive


def get_links_per_achive_pages_all_years(archive_years):
    """
    Suche in allen Archiv-Jahren auf nach Seiten, auf denen Artikel gelistet sind.
    Beispiel: Im jedem der 5 Jahre gibt es 47 Seiten - Diese 5*47 Links kommen zurück
    :param archive_years: Liste mit Links zu allen vorhandenen Archiv-Jahren
    :return: [List[List]] = [Archiv-Jahr[Alle Links aller Archiv-Seiten]]
    """
    if REFRESH_STATIC_DATA:
        links_per_achive_pages_all_years = []
        for archive_year in archive_years:
            current_year = get_links_per_archive_pages(archive_year)
            links_per_achive_pages_all_years.append(current_year)

        anything2json(links_per_achive_pages_all_years, "links_per_achive_pages_all_years")
    else:
        links_per_achive_pages_all_years = json2dict("links_per_achive_pages_all_years.json")

    logging.info("Succesfully generated catalogue for all pages-links for all articles for all years")
    return links_per_achive_pages_all_years


@limits(calls=FOLLOW_LINK_RATE, period=1)
@sleep_and_retry
def get_links_to_reports(single_archive_page):
    """
    Von einer einzelnen Seiten werden alle Links zu Artikeln gesucht und zurückgegeben
    :param archive_site: Link zu einer einzelnen Archivseite (z.B. Seite 1 von 57 aus Jahr 2016)
    :return: [List] mit den Links aller Aritkeln auf einer Seite
    """
    url = BERLIN_URL + single_archive_page
    response = requests.get(url, headers = HEADERS)

    if response.status_code != 200:
        logging.error(
            'get_links_to_reports(single_archive_page) - response: {} - url {}'.format(response.status_code, url))
        # Throw Exception, that is caught by decorater, will Retry later!
        #raise Exception('API response: {}'.format(response.status_code))

    doc = lxml.html.fromstring(response.content)
    links_to_reports = doc.xpath("//div[contains(@class, 'cell')]/a/@href")
    return links_to_reports


def get_links_to_reports_all_years(archive_years_all_pages):
    """
    Für alle Jahre im Archiv, Crawle alle Seiten und Speichere Alle Links zu Allen Artikeln
    :param archive_years: List[Liste]] - List mit allen Archiv-Jahren, die List mit allen Seiten-Links des Archivjahres enthält
    :return: [List[List]] - Pro Archiv-Jahr eine Liste, dann wieder eine Liste mit allen Report-Links verschachtelt.
    """
    links_to_reports_all_years = []
    if REFRESH_STATIC_DATA:
        for archive_year_page in archive_years_all_pages:
            links_to_reports_per_year = []
            for page in archive_year_page:
                links_to_reports_per_year = links_to_reports_per_year + get_links_to_reports(page)

            links_to_reports_all_years.append(links_to_reports_per_year)
            anything2json(links_to_reports_all_years, "links_to_reports_all_years")
    else:
        links_to_reports_all_years = json2dict("links_to_reports_all_years.json")

    logging.info("Sucesfully crawled all links to all articles on all pages for all years")
    return links_to_reports_all_years


@sleep_and_retry
@limits(calls=1, period=5)
def get_dict_from_scraped(source):
    url = BERLIN_URL + source
    response = requests.get(url, headers = HEADERS)
    if response.status_code != 200:
        logging.warning(
            'Crawl on single page information failed - response: {} - url {}'.format(response.status_code, url))
        # Throw Exception, that is caught by decorater, will Retry later!
        #raise Exception('API response: {}'.format(response.status_code))

    doc2 = lxml.html.fromstring(response.content)

    article_headline = doc2.xpath("//h1[@class = 'title']/text()")
    article_time_place = doc2.xpath("//div[@class = 'polizeimeldung']/text()")
    article_subheads = doc2.xpath("//div[@class = 'textile']/p/strong/text()")
    article_paragraphs_inclu_caps = doc2.xpath(
        "//div[@class = 'textile']/p/text() | //div[@class = 'textile']//span[@class = 'caps']/text()")

    published = article_time_place[0].strip() if article_time_place else ''

    bezirk = article_time_place[1].strip() if (len(article_time_place) > 1) else ''
    # article = "".join(article_paragraphs_inclu_caps).strip("\n ")
    article = " ".join("".join(article_paragraphs_inclu_caps).split())
    head = "".join(article_headline)
    # subs = "".join(article_subheads)

    if article_subheads:
        subs = article_subheads
    else:
        subs = " "

    if subs[0].startswith(("Treptow", "Mitte", "Neukölln", "Tempelhof", "Marzahn", "Spandau", "Steglitz",
                           "Reinickendorf", "Charlottenburg", "Pankow", "Lichtenberg",
                           "Friedrichshain")) and bezirk == "":
        bezirk = subs[0]
        subs = subs[1:]

    polizei_data = {
        "response": response.status_code,
        "headline": head,
        "published": published,
        "bezirk": bezirk,
        "subheads": subs,
        "article": article,
        "url": url
    }

    return polizei_data


def get_data_from_url_list(links):
    scraped_list_of_dicts = []
    if not LIVE:
        # If not Live, the amount of requests will be sliced down to the first 5 links of each year
        links = [sublist[:5] for sublist in links]
    for year_list in links:
        for link in year_list:
            url = BERLIN_URL + link
            if is_item_already_in_Database(url):
                logging.info(
                    "Skipping {} because already 200 in Database".format(url))
                data = None
            else:
                data = get_dict_from_scraped(link)
            if data is not None:
                dict2json3_live(data, "all_reports_all_years_all_pages")

        logging.info("-------------- Finished Crawling one more year --------------")

        scraped_list_of_dicts.append(data)

    json_output = {"items": scraped_list_of_dicts}
    dict2json3_live(json_output, "all_reports_all_years_all_pages")
    return scraped_list_of_dicts


archives_by_year = get_archives()
links_per_archive_pages = get_links_per_achive_pages_all_years(archives_by_year)
links_to_reports_all_years = get_links_to_reports_all_years(links_per_archive_pages)
page_information = get_data_from_url_list(links_to_reports_all_years)

logging.info('-------------- End of Crawling --------------')
