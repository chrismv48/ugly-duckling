
from db_models import Yelp, convert_query_results

__author__ = 'carmstrong'

import requests
from lxml import etree
import random
import re
import time
import logging
import pandas as pd
import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from multiprocessing.pool import ThreadPool

from requests.exceptions import ConnectionError, ProxyError

# logging.basicConfig(filename='yelp-scraping-log.log', format="%(asctime)s;%(levelname)s;%(message)s",
#                               datefmt="%Y-%m-%d %H:%M:%S")
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

def generate_offsets(offset, increment, num_results):
    offsets = []
    while offset + increment < min(num_results, 1000):
        offset += increment
        offsets.append(offset)

    return offsets


class YelpRequest(object):
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36',
        'Mozilla/5.0 (compatible; MSIE 10.6; Windows NT 6.1; Trident/5.0; InfoPath.2; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 2.0.50727) 3gpp-gba UNTRUSTED/1.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0']

    AUTH = requests.auth.HTTPProxyAuth('chrismv48', 'Dockside6')
    PROXY_LIST = ['us-ca.proxymesh.com:31280',
                  'us-nv.proxymesh.com:31280',
                  'us-il.proxymesh.com:31280',
                  'us.proxymesh.com:31280',
                  'uk.proxymesh.com:31280']
                  #'open.proxymesh.com:31280']
    PROXIES = {'http': 'us-nv.proxymesh.com:31280'}

    EXCLUDED_IPS = []

    def __init__(self,
                 auth=AUTH,
                 headers={'User-Agent': random.choice(USER_AGENTS)},
                 proxies=PROXIES):
        self.url = None
        self.auth = auth
        self.proxies = proxies
        self.headers = headers

    def get(self, url, max_retries=7, sleep_time=1.5):
        self.url = url
        if max_retries > 0:
            try:
                self.proxies['http'] = random.choice(self.PROXY_LIST)
                self.headers['User-Agent'] = random.choice(self.USER_AGENTS)
                self.resp = requests.get(url=self.url, auth=self.auth,
                                         headers=self.headers,
                                         proxies=self.proxies)
                try:
                    self.ip_used = self.resp.headers['x-proxymesh-ip']
                    #print "Using ip: {}" .format(self.ip_used)

                except KeyError:
                    print self.resp.headers
                    print self.resp.url
                    print self.resp.status_code
            except (ConnectionError, ProxyError) as e:
                print e
                max_retries -= 1
                time.sleep(sleep_time)
                self.get(self.url, max_retries)
            if self.resp.status_code == 503:
                blacklisted_ip = self.resp.headers['x-proxymesh-ip']
                print "ip blocked: {}".format(blacklisted_ip)
                self.EXCLUDED_IPS.append(blacklisted_ip)
                self.headers["X-ProxyMesh-Not-IP"] = ",".join(set(self.EXCLUDED_IPS))
                max_retries -= 1
                time.sleep(sleep_time)
                self.get(url=self.url, max_retries=max_retries)
            elif str(self.resp.status_code).startswith("4") or str(self.resp.status_code).startswith("5"):
                print "HTTP Error: {} {}".format(self.resp.status_code, self.resp.reason)
                max_retries -= 1
                time.sleep(sleep_time)
                self.get(url=self.url, max_retries=max_retries)

            return self.resp
        else:
            raise Exception("Max retries exceeded")


class YelpZipCode(object):
    def __init__(self, zip_code):
        self.zip_code = zip_code

    def search_url(self, offset):
        return "http://www.yelp.com/search?find_loc={}&cflt=restaurants&start={}".format(self.zip_code, offset)

    def num_search_results(self, html):
        tree = etree.fromstring(html, etree.HTMLParser())
        num_results_xpath = tree.xpath("//span[@class='pagination-results-window']", smart_strings=False)

        num_results = num_results_xpath[0].text.strip()

        results = re.search('of (\d+)', num_results)
        if results:
            return int(results.groups()[0])
        else:
            return None

    def scrape_business_page(self, html):
        tree = etree.fromstring(html, etree.HTMLParser())
        page_business_results = tree.xpath('//div[@class="search-result natural-search-result"]')
        business_results = []
        for business_result in page_business_results:
            business_name_xpath = business_result.xpath(".//a[@class='biz-name']", smart_strings=False)
            bus_name = business_name_xpath[0].text
            bus_href = 'http://www.yelp.com/' + business_name_xpath[0].attrib['href']

            business_review_xpath = business_result.xpath(".//span[@class='review-count rating-qualifier']", smart_strings=False)
            if business_review_xpath:
                bus_reviews_count = int(re.sub('\D+', '', business_review_xpath[0].text))
            else:
                bus_reviews_count = 0

            business_results.append({"business_name": bus_name,
                                     "business_href": bus_href,
                                     "business_review_count": bus_reviews_count})
        return business_results

    def scrape_review_page(self, html):
        tree = etree.fromstring(html, etree.HTMLParser())
        reviews = tree.xpath('//div[@class="review-content"]', smart_strings=False)
        review_data = []
        for review in reviews:
            date_published = review.xpath('.//meta[@itemprop="datePublished"]', smart_strings=False)[0].attrib['content']
            review_rating = float(review.xpath('.//meta[@itemprop="ratingValue"]', smart_strings=False)[0].attrib['content'])
            review_data.append({
                "date_published": date_published,
                "review_rating": review_rating})

        return review_data

    def generate_links(self, url_prefix, num_search_results, increment, offset=0):
        offset_array = generate_offsets(offset=offset, increment=increment, num_results=num_search_results)
        url_array = [url_prefix + offset for offset in offset_array]

        return url_array


DB_LOCATION = 'sqlite:///yelp.db'
_engine = create_engine(DB_LOCATION)
_DBSession = sessionmaker(bind=_engine)
# to execute queries. Possibly better way to facilitate this?
session = _DBSession()

zip_codes_df = pd.read_excel('Sampled Zip Codes.xlsx')
zip_codes_df['RegionName'] = zip_codes_df['RegionName'].apply(lambda x: "0" * (5 - len(str(int(x)))) + str(int(x)))
zip_codes = list(zip_codes_df['RegionName'])
db_zip_codes = session.query(Yelp).all()
db_zip_codes = convert_query_results(db_zip_codes)
db_zip_codes_list = [i['zip_code'] for i in db_zip_codes]

subset = zip_codes
zip_batch = set(subset) - set(db_zip_codes_list)
print "{} zip codes already exist in db; filtering out.".format(len(subset) - len(zip_batch))
yelp_req = YelpRequest()
for i, zip_code in enumerate(zip_batch):

    start_time = datetime.datetime.now()

    print "Scraping zip code {} ({} out of {})".format(zip_code, i, len(zip_batch))

    yelp_zip = YelpZipCode(zip_code)
    first_page_resp = yelp_req.get(yelp_zip.search_url(offset=0))
    first_page_html = first_page_resp.content
    num_results = yelp_zip.num_search_results(first_page_html)
    print "Number of businesses: {}".format(num_results)
    search_offsets = generate_offsets(offset=0, increment=10, num_results=num_results)

    if search_offsets:
        search_urls = [yelp_zip.search_url(offset=offset) for offset in search_offsets]
        pool = ThreadPool(6)
        subsequent_bus_page_responses = pool.map(yelp_req.get, search_urls)
        pool.close()
        pool.join()
    # TODO: this may not be computed if no search_offsets?
    subsequent_pages_html = [resp.content for resp in subsequent_bus_page_responses]

    zip_code_business_results = []
    for page_html in subsequent_pages_html + [first_page_html]:
        zip_code_business_results.extend(yelp_zip.scrape_business_page(page_html))

    review_urls = []
    for business_result in zip_code_business_results:
        review_offsets = generate_offsets(offset=0, increment=40, num_results=business_result['business_review_count'])
        review_urls.extend([business_result['business_href'] + '?start=' + str(offset) for offset in review_offsets])

    review_urls.extend([business['business_href'] for business in zip_code_business_results])
    print "Number of review links: {}".format(len(review_urls))

    pool = ThreadPool(6)
    review_responses = pool.map(yelp_req.get, review_urls)
    pool.close()
    pool.join()

    review_results = []
    for review_response in review_responses:
        review_results.extend(yelp_zip.scrape_review_page(review_response.content))

    df = pd.DataFrame(review_results)
    grouped_df = df.groupby(df['date_published'].str[:-3]).agg({"date_published": "count", "review_rating": "mean"})
    grouped_df['zip_code'] = zip_code
    grouped_df = grouped_df.rename(columns={"date_published": "num_reviews"})
    grouped_df['review_rating'] = grouped_df['review_rating'].round(2)
    grouped_df = grouped_df.reset_index()

    for row in grouped_df.T.to_dict().values():
        session.merge(Yelp(**row))
        session.commit()

    num_search_requests = round(len(zip_code_business_results) / 10.0, 0)
    total_requests = num_search_requests + len(review_urls)
    run_time = datetime.datetime.now() - start_time
    print "Completed {} requests in {}s ({} rps)".format(int(total_requests),
                                                         run_time.seconds,
                                                         round(total_requests / run_time.seconds, 2))
    print "Zip code {} finished. Sleeping...\n".format(zip_code)
    #time.sleep(52)
print 'done'