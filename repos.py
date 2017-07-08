# -*- coding: utf-8 -*-
import base64


def episode_key(episode_url):
    return "episode:%s" % (episode_url)


def text_url2episode_key(gs_url):
    return "text:%s:episode" % gs_url


def word2episodes_key(entity_name):
    base = base64.b64encode(entity_name)
    return "word:%s" % base


def get_all_words(r):
    return r.smembers(ALL_WORD_KEY)


ALL_WORD_KEY = "all-words"


def get_episode(r, episode_url):
    key = episode_key(episode_url)
    return r.hgetall(key)


def store_episode(r, title, episode_url, contributors, duration, published, enclosure):
    key = episode_key(episode_url)
    names = [c['name'] for c in contributors]
    r.hmset(key, {
        'episode_url': episode_url,
        'title': title,
        'duration': duration,
        'contributors': ','.join(names),
        'published': published,
        'enclosure': enclosure,
    })


def get_episodes(r, word):
    return r.smembers(word2episodes_key(word))
