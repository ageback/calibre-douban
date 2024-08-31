import re
import time
import random
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue, Empty
from urllib.parse import urlparse, unquote, urlencode
from urllib.request import Request, urlopen

# from calibre import random_user_agent
# from calibre.ebooks.metadata import check_isbn
# from calibre.ebooks.metadata.book.base import Metadata
# from calibre.ebooks.metadata.sources.base import Source, Option
from lxml import etree

DOUBAN_BOOK_BASE = "https://book.douban.com/"
DOUBAN_SEARCH_JSON_URL = "https://www.douban.com/j/search"
DOUBAN_SEARCH_URL = "https://www.douban.com/search"
DOUBAN_BOOK_URL = 'https://book.douban.com/subject/%s/'
DOUBAN_BOOK_CAT = "1001"
DOUBAN_CONCURRENCY_SIZE = 5  # 并发查询数
DOUBAN_BOOK_URL_PATTERN = re.compile(".*/subject/(\\d+)/?")
PROVIDER_NAME = "New Douban Books"
PROVIDER_ID = "new_douban"
PROVIDER_VERSION = (2, 1, 0)
PROVIDER_AUTHOR = 'Gary Fu'


def get_headers():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0',
               'Accept-Encoding': 'gzip, deflate'}
    return headers


def get_res_content(res):
    encoding = res.info().get('Content-Encoding')
    if encoding == 'gzip':
        res_content = gzip.decompress(res.read())
    else:
        res_content = res.read()
    return res_content.decode(res.headers.get_content_charset())


def calc_url(href):
    query = urlparse(href).query
    params = {item.split('=')[0]: item.split('=')[1] for item in query.split('&')}
    url = unquote(params['url'])
    if DOUBAN_BOOK_URL_PATTERN.match(url):
        return url


def get_tags(book_content):
    tag_pattern = re.compile("criteria = '(.+)'")
    tag_match = tag_pattern.findall(book_content)
    if len(tag_match):
        return [tag.replace('7:', '') for tag in
                filter(lambda tag: tag and tag.startswith('7:'), tag_match[0].split('|'))]
    return []


def load_book_urls_new(query):
    params = {"cat": DOUBAN_BOOK_CAT, "q": query}
    url = DOUBAN_SEARCH_URL + "?" + urlencode(params)
    res = urlopen(Request(url, headers=get_headers(), method='GET'))
    book_urls = []
    if res.status in [200, 201]:
        html_content = get_res_content(res)
        html = etree.HTML(html_content)
        alist = html.xpath('//a[@class="nbg"]')
        for link in alist:
            href = link.attrib['href']
            parsed = calc_url(href)
            if parsed:
                if len(book_urls) < 100:
                    book_urls.append(parsed)
    return book_urls


def get_tail(element, default_str=''):
    text = default_str
    if isinstance(element, etree._Element) and element.tail:
        text = element.tail.strip()
        if not text:
            text = get_text(element.getnext(), default_str)
    return text if text else default_str


def get_text(element, default_str=''):
    text = default_str
    if len(element) and element[0].text:
        text = element[0].text.strip()
    elif isinstance(element, etree._Element) and element.text:
        text = element.text.strip()
    return text if text else default_str


def get_rating(rating_element):
    return float(get_text(rating_element, '0')) / 2


def author_filter(a_element):
    a_href = a_element.attrib['href']
    return '/author' in a_href or '/search' in a_href


def parse_book(url, book_content):
    book = {}
    html = etree.HTML(book_content)
    if html is None or html.xpath is None:  # xpath判空处理
        return None
    title_element = html.xpath("//span[@property='v:itemreviewed']")
    book['title'] = get_text(title_element)
    share_element = html.xpath("//a[@data-url]")
    if len(share_element):
        url = share_element[0].attrib['data-url']
    book['url'] = url
    id_match = DOUBAN_BOOK_URL_PATTERN.match(url)
    if id_match:
        book['id'] = id_match.group(1)
    img_element = html.xpath("//a[@class='nbg']")
    if len(img_element):
        cover = img_element[0].attrib['href']
        if not cover or cover.endswith('update_image'):
            book['cover'] = ''
        else:
            book['cover'] = cover
    rating_element = html.xpath("//strong[@property='v:average']")
    book['rating'] = get_rating(rating_element)
    elements = html.xpath("//span[@class='pl']")
    book['authors'] = []
    book['translators'] = []
    book['publisher'] = ''
    for element in elements:
        text = get_text(element)
        if text.startswith("作者"):
            book['authors'].extend([get_text(author_element) for author_element in
                                    filter(author_filter, element.findall("..//a"))])
        elif text.startswith("译者"):
            book['translators'].extend([get_text(translator_element) for translator_element in
                                        filter(author_filter, element.findall("..//a"))])
        elif text.startswith("出版社"):
            book['publisher'] = get_tail(element)
        elif text.startswith("副标题"):
            book['title'] = book['title'] + ':' + get_tail(element)
        elif text.startswith("出版年"):
            book['publishedDate'] = get_tail(element)
        elif text.startswith("ISBN"):
            book['isbn'] = get_tail(element)
        elif text.startswith("丛书"):
            book['series'] = get_text(element.getnext())
    summary_element = html.xpath("//div[@id='link-report']//div[@class='intro']")
    book['description'] = ''
    if len(summary_element):
        book['description'] = etree.tostring(summary_element[-1], encoding="utf8").decode("utf8").strip()
    tag_elements = html.xpath("//a[contains(@class, 'tag')]")
    if len(tag_elements):
        book['tags'] = [get_text(tag_element) for tag_element in tag_elements]
    else:
        book['tags'] = get_tags(book_content)
    book['source'] = {
        "id": PROVIDER_ID,
        "description": PROVIDER_NAME,
        "link": DOUBAN_BOOK_BASE
    }
    return book


def load_book(url):
    book = None
    start_time = time.time()
    res = urlopen(Request(url, headers=get_headers(), method='GET'))
    if res.status in [200, 201]:
        print("Downloaded:{} Successful,Time {:.0f}ms".format(url, (time.time() - start_time) * 1000))
        book_detail_content = get_res_content(res)
        book = parse_book(url, book_detail_content)
    return book


def search_books(query):
    book_urls = load_book_urls_new(query)
    books = []
    thread_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix='douban_async')
    futures = [thread_pool.submit(load_book, book_url) for book_url in book_urls]
    for future in as_completed(futures):
        book = future.result()
        if book is not None:
            books.append(future.result())
    return books


search_books("9787111544937")
