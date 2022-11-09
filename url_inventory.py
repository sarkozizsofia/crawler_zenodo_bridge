#!/usr/bin/env python3
# -*- coding: utf-8, vim: expandtab:ts=4
import re
from datetime import datetime
from pathlib import Path
from threading import Lock as threading_Lock
from bs4 import BeautifulSoup
import yaml
from warcio.archiveiterator import ArchiveIterator
LOCALE_LOCK = threading_Lock()


def response_warc_record_gen(warc_filename):
    archive_iter = ArchiveIterator(open(warc_filename, 'rb'))
    for rec in archive_iter:
        if rec.rec_type == 'response':
            warc_response_date = rec.rec_headers.get_header('WARC-Date')
            if '.' in warc_response_date:
                date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
            else:
                date_format = '%Y-%m-%dT%H:%M:%SZ'
            article_url = rec.rec_headers.get_header('WARC-Target-URI')
            warc_response_datetime = datetime.strptime(warc_response_date, date_format)
            warc_id = rec.rec_headers.get_header('WARC-Record-ID')
            # Content-Length
            raw_html = rec.content_stream().read().decode(rec.rec_headers.get_header('WARC-X-Detected-Encoding'))
            yield article_url, warc_response_datetime, raw_html


def get_url_data_from_warc(dirnb, warc_name, next_p_fun, multipage_comp):
    with open(f'URL_inventory', 'w') as yamlfile:
        url_info_dict = {}
        warcpath = f'{dirname}/{warc_name}'
        urls, first = [], ''
        for a_url, warc_resp_date, raw_html in response_warc_record_gen(warcpath):
            url_info_dict[a_url] = {'warc': warc_name, 'page_cat': 'simple', 'next_pages': [], 'config': 'config',
                                    'warc_date': warc_resp_date}
            is_multi = multipage_comp.match(a_url)
            is_next_p = next_p_fun(raw_html)
            if is_multi:
                url_info_dict[a_url]['page_cat'] = 'ignore'
            if is_next_p:
                if len(urls) == 0:
                    url_info_dict[a_url]['page_cat'] = 'multi'
                    first = a_url
                urls.append(is_next_p)
            else:
                if len(urls) > 0:  # az utolsó többedik oldal / egy másik oldal
                    # TODO: ez így akkor működik, ha nem ékelődik más az összetartozó oldalak közé az iterációkor
                    url_info_dict[first]['next_pages'] = urls
                    urls, first = [], ''
    for k, v in url_info_dict.items():
        print(k, v)
    # url_yaml = yaml.dump(url_info_dict)# , sort_keys=True)
    with open('telex_koronavirus_urls.yaml', 'w') as outfile:
        yaml.dump(url_info_dict, outfile, default_flow_style=False)


def next_page_of_article_telex(curr_html):  # https://telex.hu/koronavirus/2020/11/12/koronavirus-pp-2020-11-12/elo
    bs = BeautifulSoup(curr_html, 'lxml')
    if bs.find('div', class_='pagination') is not None:
        current_pagenum = int(bs.find('a', class_='current-page').attrs['href'][-1])
        for pagelink in bs.find_all('a', class_='page'):
            if pagelink.attrs['class'] != ['page', 'current-page']:
                href = pagelink.attrs['href']
                if href[-1].isdigit() and int(href[-1]) == current_pagenum + 1:
                    next_page = f'https://telex.hu{href}'
                    return next_page
    return None


if __name__ == '__main__':
    multipage_url_end = re.compile(r'.*oldal=.')
    dirname = '/media/dh/6EAB565C0EA732DB/warcs_dir'
    warcname = 'telex_koronavirus-articles_new.warc.gz'
    get_url_data_from_warc(dirname, warcname, next_page_of_article_telex, multipage_url_end)
