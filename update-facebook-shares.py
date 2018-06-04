#!/usr/local/bin/python3

import datetime
from elasticsearch import Elasticsearch
import certifi
import requests
from dateutil.relativedelta import relativedelta as rd
import sys
import time
from elasticsearch.helpers import scan
from elasticsearch.helpers import bulk

ES_INSTANCE = Elasticsearch(
    ['https://some-fake-instance.eu-west-1.aws.found.io/'],
    http_auth=('analytics-user', 'some-password'),
    port=9243,
    use_ssl=True,
    verify_certs=True,
    ca_certs=certifi.where(),
    request_timeout=30,
    retry_on_timeout=True
)


current_time = datetime.datetime.utcnow()
start_date = current_time - rd(minutes=30)
end_date = current_time + rd(hours=1)


## Get viewed articles
print('Getting recently viewed articles...')
if len(sys.argv) > 1:
    # start date can be overwritten as a command-line argument
    start_date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d:%H')
articles = {}

results = ES_INSTANCE.search(
    body={'query':{'range':{'date':{'gte':start_date,'lte':end_date}}},"aggs":{"articles":{"terms":{"field":"nid","size":10000,"min_doc_count":5}}},'size':0},index='hivisasa.com',doc_type='analytics')
for article_stats in results.get('aggregations').get('articles').get('buckets'):
    nid = article_stats.get('key')
    page_views = article_stats.get('doc_count')
    articles[nid] = {}
print('Found {} recently viewed articles...'.format(len(articles)))


## Add page view data
print('Getting page views...')
results = ES_INSTANCE.search(
    index='hivisasa.com',
    doc_type='analytics',
    body={
        'query' : {
          'constant_score' : {
            'filter': {
              'terms': {
                'nid': list(articles.keys())}}}},
        'size' : 0,
        'aggs': {
          'page_views': {
            'terms': {
              'field': 'nid',
              'size': 10000}}}})
page_view_bukcets = results.get("aggregations").get("page_views").get("buckets")
print("Found page views for {} articles...".format(len(page_view_bukcets)))
for page_view_stats in page_view_bukcets:
    nid = page_view_stats.get("key")
    page_views = page_view_stats.get("doc_count")
    articles[nid] = {"page_views": page_views}


## Add article data
print('Fetching article details...')
articleDocs = ES_INSTANCE.search(
    index='articles',
    doc_type='article',
    body={
        'query' : {
          'ids' : {
            'type' : 'article',
            'values' : list(articles.keys())
          }
        },
        'size' : 10000,
        '_source': [
          'slug', 'writer_id', 'title', 'kcategory',
          'klocation', 'publish_date']})
articleDocs = articleDocs.get('hits').get('hits')
print('Found data for {} articles...'.format(len(articleDocs)))
for articleDoc in articleDocs:
    slug = get_field(articleDoc, 'slug')
    writer_id = get_field(articleDoc, 'writer_id')
    title = get_field(articleDoc, 'title')
    category = get_field(articleDoc, 'kcategory')
    county = get_field(articleDoc, 'klocation')
    publish_date = get_field(articleDoc, 'publish_date')
    if slug:
        articles[articleDoc.get('_id')]['url'] = (
            'https://hivisasa.com/posts/' + slug)
        articles[articleDoc.get('_id')]['writer_id'] = writer_id
        articles[articleDoc.get('_id')]['title'] = title
        articles[articleDoc.get('_id')]['category'] = category
        articles[articleDoc.get('_id')]['county'] = county
        articles[articleDoc.get('_id')]['publish_date'] = publish_date
    else:
        print('Failed to get slug for: {}'.format(articleDoc.get('_id')))
        articles.pop(articleDoc.get('_id'))


## Populate share counts
print('Fetching engagement data...')
apikey = 'some-fake-key'
count = 0
for article_id in list(articles.keys()):
    count += 1
    article = articles[article_id]
    params = {'apikey': apikey, 'url': article.get('url')}
    analytics_request = requests.get('https://plus.sharedcount.com/url', params=params)
    total_fb_shares = analytics_request.json()['Facebook']['share_count']
    total_fb_comments = analytics_request.json()['Facebook']['comment_count']
    article['shares'] = total_fb_shares
    article['comments'] = total_fb_comments
    if count % 10 == 0:
        print('...processed {} articles'.format(count))


## Upload stats
print('Uploading stats...')
article_stats = []
for article_id in articles:
    print(article_id)
    article = articles.get(article_id)
    article_stats.append({
        '_op_type': 'index',
        '_index': 'social_engagements',
        '_type': 'article_engagement',
        '_id': article_id,
        '_source': {
             'publish_date': article.get('publish_date'),
             'writer_id': article.get('writer_id'),
             'title': article.get('title'),
             'article_id': article_id,
             'updated_ts': current_time,
             'shares': article.get('shares'),
             'comments': article.get('comments'),
             'category': article.get('category'),
             'county': article.get('county'),
             'page_views': article.get('page_views')}})
bulk(ES_INSTANCE, iter(article_stats))
print('Processed {} articles from {} to {}'.format(
    len(articles), start_date, end_date))
def get_field(doc, field_name):
    return doc.get('_source', {}).get(field_name, [''])
