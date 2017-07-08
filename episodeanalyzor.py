# -*- coding: utf-8 -*-
import codecs
import errno
import os
import time
from urlparse import urlparse, urlsplit

import requests
import ffmpy

from google.cloud import storage, speech, language

import repos


TARGET_BUCKET = "podcast-search"
SPEECH_MAX_ALTERNATIVES = 2


def convert_episode_to_flac(podcast_name, episode_url, dist_dir):
    sp = -2 if episode_url.endswith("/") else -1
    filename = episode_url.split('/')[sp]

    pardir = os.path.join(dist_dir, podcast_name)
    try:
        os.makedirs(pardir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(pardir):
            pass
        else:
            raise

    local_row_path = os.path.join(pardir, filename)
    local_filname, _ = os.path.splitext(local_row_path)
    local_flac_path = "%s.%s" % (local_filname, 'flac')
    flac_filename = os.path.basename(local_flac_path)
    objname = os.path.join(podcast_name, flac_filename)
    source_uri = "gs://%s/%s" % (TARGET_BUCKET, objname)
    dist_uri = "gs://%s/%s/%s" % (TARGET_BUCKET, podcast_name, flac_filename)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(TARGET_BUCKET)
    blob = bucket.blob(objname)

    if blob.exists():
        # Skip convetion
        return dist_uri

    # https://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
    if not os.path.exists(local_row_path):
        r = requests.get(episode_url, stream=True)
        with open(local_row_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)

    # Translate mp3 to FLAC
    # MEMO : MP3 sampling rate of Rebuild.fm is 44100 Hz.
    ff = ffmpy.FFmpeg(
         inputs={local_row_path: None},
         outputs={local_flac_path: None}
         # outputs={local_flac_path: '-y'}
     )
    ff.run()

    blob.upload_from_filename(local_flac_path)
    return dist_uri


def recognize_episode_text(podcast_name, source_uri, dist_dir):
    filname = source_uri.split('/')[-1]
    tx_filename = "%s.%s" % (filname, 'txt')
    local_tx_path = os.path.join(dist_dir, podcast_name, tx_filename)

    if not os.path.exists(local_tx_path):
        # https://googlecloudplatform.github.io/google-cloud-python/stable/speech-usage.html
        speech_client = speech.Client()
        sample = speech_client.sample(
                source_uri=source_uri, encoding=speech.Encoding.FLAC, sample_rate_hertz=44100)
        op = sample.long_running_recognize(
                language_code='ja-JP',
                max_alternatives=SPEECH_MAX_ALTERNATIVES,
            )

        retry_count = 0
        while not op.complete:
            retry_count += 1
            time.sleep(10)
            op.poll()
            print("Recognize episode ... ", retry_count)

        with codecs.open(local_tx_path, "w", "utf-8") as f:
            for result in op.results:
                max_confidence = 0.0
                best_alt = None
                for i, alt in enumerate(result.alternatives):
                    if max_confidence <= alt.confidence:
                        best_alt = alt
                f.write(best_alt.transcript)

    objname = os.path.join(podcast_name, tx_filename)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(TARGET_BUCKET)
    blob = bucket.blob(objname)
    blob.upload_from_filename(local_tx_path)
    dist_uri = "gs://%s/%s" % (TARGET_BUCKET, objname)
    return dist_uri


def analyze_text_entity(podcast_name, gs_url, redis):
    client = language.Client()
    doc = client.document_from_url(gs_url, language='ja', encoding=language.Encoding.UTF16)
    res = doc.analyze_entities()

    episode_url = redis.get(repos.text_url2episode_key(gs_url))
    for e in res.entities:
        entity_name = e.name.encode('utf-8')
        redis.sadd(repos.word2episodes_key(entity_name), episode_url)
        redis.sadd(repos.ALL_WORD_KEY, e.name)
