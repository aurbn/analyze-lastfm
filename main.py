#!/usr/bin/env python

import sys
import os
import os.path
import json
import cjson
from time import sleep, mktime
from datetime import datetime
from collections import namedtuple, Counter
from xml.etree import ElementTree
from hashlib import md5

import requests
requests.packages.urllib3.disable_warnings()

import seaborn as sns
import pandas as pd


LASTFM_DIR = 'lastfm'
LASTFM_API = 'http://ws.audioscrobbler.com/2.0/'
LASTFM_KEY = 'bd221fa33740b25dcce42dac36c86b60'
LASTFM_TRACKS = 'lastfm_tracks.json'

ECHONEST_DIR = 'echonest'
ECHONEST_API = 'http://developer.echonest.com/api/v4/'
ECHONEST_KEY = 'L5WW5JLI1ZVGAPJQW'


Artist = namedtuple('Artist', ['name', 'image'])
Album = namedtuple('Album', ['name', 'image'])
LastfmTrack = namedtuple(
    'LastfmTrack',
    ['artist', 'album', 'name', 'timestamp', 'loved']
)


def call_lastfm(**parameters):
    parameters['api_key'] = LASTFM_KEY
    response = requests.get(
        LASTFM_API,
        params=parameters
    )
    return response.content


def download_lastfm_tracks_page(page, user='AlexKuk'):
    print >>sys.stderr, 'Download lastfm tracks for {user}, page: {page}'.format(
        user=user,
        page=page
    )
    return call_lastfm(
        method='user.getRecentTracks',
        limit=200,
        page=page,
        user=user,
        extended=1,
    )


def get_lastfm_tracks_page_filename(page):
    return '{page}.xml'.format(page=page)


def parse_lastfm_tracks_page_filename(filename):
    page, _ = os.path.splitext(filename)
    return int(page)


def get_lastfm_tracks_page_path(page):
    filename = get_lastfm_tracks_page_filename(page)
    return os.path.join(LASTFM_DIR, filename)


def load_lastfm_tracks_page(page):
    path = get_lastfm_tracks_page_path(page)
    with open(path) as file:
        return file.read()


def dump_lastfm_tracks_page(data, page):
    path = get_lastfm_tracks_page_path(page)
    with open(path, 'w') as file:
        file.write(data)


def parse_timestamp(timestamp):
    if timestamp:
        return datetime.fromtimestamp(timestamp)


def parse_lastfm_tracks_page(data):
    xml = ElementTree.fromstring(data)
    for track in xml.find('recenttracks'):
        artist = track.find('artist')
        artist_name = artist.findtext('name')
        album_image = artist.findtext('image[@size="extralarge"]') or None
        loved = bool(int(track.findtext('loved')))
        name = track.findtext('name')
        album_name = track.findtext('album')
        artist_image = track.findtext('image[@size="extralarge"]')
        timestamp = track.find('date')
        if timestamp is not None:
            timestamp = timestamp.attrib['uts']
            timestamp = parse_timestamp(int(timestamp))
        yield LastfmTrack(
            Artist(artist_name, artist_image),
            Album(album_name, album_image),
            name, timestamp, loved
        )


def list_lastfm_tracks_pages():
    for filename in os.listdir(LASTFM_DIR):
        yield parse_lastfm_tracks_page_filename(filename)


def load_lastfm_tracks():
    for index, page in enumerate(list_lastfm_tracks_pages()):
        if index > 0 and index % 100 == 00:
            print >>sys.stderr, 'Loading file #{index}'.format(
                index=index
            )
        data = load_lastfm_tracks_page(page)
        for track in parse_lastfm_tracks_page(data):
            yield track


def serialize_timestamp(timestamp):
    if timestamp:
        return mktime(timestamp.timetuple())


def dump_lastfm_tracks(tracks, path=LASTFM_TRACKS):
    with open(path, 'w') as file:
        data = [
            ((_.artist.name, _.artist.image),
             (_.album.name, _.album.image),
             _.name, serialize_timestamp(_.timestamp), _.loved)
            for _ in tracks]
        file.write(cjson.encode(data))


def load_lastfm_tracks(path=LASTFM_TRACKS):
    with open(path) as file:
        data = cjson.decode(file.read())
        return [
            LastfmTrack(
                Artist(artist_name, artist_image),
                Album(album_name, album_image),
                name, parse_timestamp(timestamp), loved
            )
            for ((artist_name, artist_image),
                 (album_name, album_image),
                 name, timestamp, loved)
            in data
        ]


def call_echonest(method, **parameters):
    parameters['api_key'] = ECHONEST_KEY
    parameters['format'] = 'json'
    response = requests.get(
        ECHONEST_API + method,
        params=parameters
    )
    return response.json()


def download_echonest_track_serp(artist, track):
    print >>sys.stderr, u'Search at Echonest "{artist} - {track}"'.format(
        artist=artist,
        track=track
    )
    return call_echonest(
        'song/search',
        results=100,
        artist=artist,
        title=track,
        bucket=['audio_summary', 'artist_discovery',
                'artist_discovery_rank', 'artist_familiarity',
                'artist_familiarity_rank', 'artist_hotttnesss',
                'artist_hotttnesss_rank', 'artist_location',
                'song_currency', 'song_currency_rank', 'song_hotttnesss',
                'song_hotttnesss_rank', 'song_type']
    )


def get_artist_track_hash(artist, track):
    hash = artist + track
    hash = md5(hash.encode('utf8')).hexdigest()
    return hash


def get_echonest_track_serp_filename(artist, track):
    return '{hash}.json'.format(
        hash=get_artist_track_hash(artist, track)
    )


def get_echonest_track_serp_path(artist, track):
    filename = get_echonest_track_serp_filename(artist, track)
    return os.path.join(ECHONEST_DIR, filename)


def load_echonest_track_serp(artist, track):
    path = get_echonest_track_serp_path(artist, track)
    with open(path) as file:
        return json.load(file)


def dump_echonest_track_serp(serp, artist, track):
    path = get_echonest_track_serp_path(artist, track)
    with open(path, 'w') as file:
        json.dump(serp, file)


if __name__ == '__main__':
    tracks = load_lastfm_tracks()
    artist_tracks = Counter((_.artist.name, _.name) for _ in tracks)
    for (artist, track), _ in artist_tracks.most_common():
        path = get_echonest_track_serp_path(artist, track)
        if not os.path.exists(path):
            serp = download_echonest_track_serp(artist, track)
            dump_echonest_track_serp(serp, artist, track)
            sleep(0.5)
