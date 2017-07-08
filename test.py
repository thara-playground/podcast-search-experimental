# -*- coding: utf-8 -*-
import sys
sys.path.append('lib')

import codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

import redis
import feedparser

import episodeanalyzor as ea
import repos

r = redis.Redis('localhost', decode_responses=True)

feed_url = "http://feeds.rebuild.fm/rebuildfm"
res = feedparser.parse(feed_url)

podcast_name = res.feed.title

episode_urls = []
with r.pipeline(transaction=False) as p:
    for entry in res.entries:
        title = entry.title
        link  = entry.link
        published = entry.published
        contributors = entry.contributors
        duration = entry['itunes_duration']
        enclosure = entry.enclosures[0].href
        repos.store_episode(p, title, link, contributors, duration, published, enclosure)

        if '40' in link:
            episode_urls.append((link, enclosure))
    p.execute()

dist_dir = "dist"

for episode_url, enclosure in episode_urls:

# episode_url = "http://rebuild.fm/40/"
# url = "http://cache.rebuild.fm/podcast-ep40.mp3"
    source = ea.convert_episode_to_flac(podcast_name, enclosure, dist_dir)
    print source

    text_url = ea.recognize_episode_text(podcast_name, source, dist_dir)
    print text_url

    key = repos.text_url2episode_key(text_url)
    r.set(key, episode_url)

    ea.analyze_text_entity(podcast_name, text_url, r)


for w in repos.get_all_words(r):
    print w
