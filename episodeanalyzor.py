# -*- coding: utf-8 -*-
import os
import time

import requests
import ffmpy

from google.cloud import storage, speech, language


TARGET_BUCKET = "podcast-search"
SPEECH_MAX_ALTERNATIVES = 2


def recognize_episode_text(episode_url, dist_dir):
    sp = -2 if episode_url.endswith("/") else -1
    filename = episode_url.split('/')[sp]

    # https://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
    r = requests.get(episode_url, stream=True)
    row_path = os.path.join(dist_dir, filename)
    with open(row_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

    # Translate mp3 to FLAC
    # MEMO : MP3 sampling rate of Rebuild.fm is 44100 Hz.
    filname, ext = os.path.splitext(row_path)
    flac_path = "%s.%s" % (filname, 'flac')
    ff = ffmpy.FFmpeg(
         inputs={row_path: None},
         outputs={flac_path: None}
     )
    ff.run()

    objname = os.path.basename(flac_path)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(TARGET_BUCKET)
    blob = bucket.blob(objname)
    blob.upload_from_filename(flac_path)

    # https://googlecloudplatform.github.io/google-cloud-python/stable/speech-usage.html
    source_uri = "gs://%s/%s" % (TARGET_BUCKET, objname)
    speech_client = speech.Client()
    sample = speech_client.sample(
            source_uri=source_uri, encoding=speech.Encoding.FLAC, sample_rate_hertz=44100)
    op = sample.long_running_recognize(
            language_code='ja-JP',
            max_alternatives=SPEECH_MAX_ALTERNATIVES,
        )

    retry_count = 100
    while retry_count > 0 and not op.complete:
        retry_count -= 1
        time.sleep(10)
        op.poll()

    tx_paths = []
    for result in op.results:
        for i, alt in enumerate(result.alternatives:)
            tx_path = "%s.%s.%s" % (filname, i, 'txt')
            with open(tx_path, 'w') as f:
                f.write(alt.transcript)
            tx_paths.append(tx_path)

    gs_uris = []
    for path in tx_paths:
        objname = os.path.basename(path)
        blob = bucket.blob(objname)
        blob.upload_from_filename(path)
        dist_uri = "gs://%s/%s" % (TARGET_BUCKET, objname)
        gs_uris.append(dist_uri)
    return gs_uris


def analyze_text_entity(gs_url):
    client = language.Client()
    doc = client.document_from_url(gs_url, language='ja', encoding=language.Encoding.UTF16)
    res = doc.analyze_entities()
    for e in res.entities:
        entity_name = e.name
        entity_type = e.entity_type
        salience = entity.salience
        print(entity_name, entity_type, salience)
